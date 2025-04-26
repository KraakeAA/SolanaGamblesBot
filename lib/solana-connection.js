import { Connection } from '@solana/web3.js';

class RateLimitedConnection extends Connection {
    constructor(endpoint, options = {}) {
        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                // Use a default client ID if none provided in options
                'solana-client': `SolanaGamblesBot/2.1 (${options.clientId || 'enhanced-connection'})`,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined,
            // Note: disableRetryOnRateLimit from web3.js ConnectionConfig is respected by super,
            // but our custom logic effectively overrides it by implementing retries here.
            // We set it false for clarity, ensuring the base class doesn't interfere unexpectedly.
            disableRetryOnRateLimit: false
        };
        super(endpoint, rpcOptions);

        // --- Configuration ---
        // Allow configuring options directly or fallback to defaults
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,       // Max parallel requests
            retryBaseDelay: options.retryBaseDelay || 600,  // Initial delay for retries (ms)
            maxRetries: options.maxRetries || 5,            // Max attempts per request
            rateLimitCooloff: options.rateLimitCooloff || 5000, // Min pause duration after hitting 429 (ms)
            retryMaxDelay: options.retryMaxDelay || 30000,  // Max delay for exponential backoff (ms)
            retryJitter: options.retryJitter || 0.2         // Percentage jitter (0.2 = +/- 20%)
        };

        // --- State ---
        this.requestQueue = [];         // Holds { method, args, resolve, reject, retries }
        this.activeRequests = 0;        // Current number of in-flight requests
        this.lastRateLimitTime = 0;     // Timestamp of the last 429 error
        this.consecutiveRateLimits = 0; // Counter for consecutive 429s

        // --- Statistics ---
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0
        };

        // --- Initialization ---
        this._wrapRpcMethods();
        console.log("RateLimitedConnection initialized with options:", this.options);
    }

    _wrapRpcMethods() {
        // List of common RPC methods to wrap (add others if needed)
        const rpcMethods = [
            'getAccountInfo', 'getBalance', 'getBlock', 'getBlockHeight',
            'getBlockProduction', 'getBlockCommitment', 'getBlocks', 'getBlocksWithLimit',
            'getBlockTime', 'getClusterNodes', 'getEpochInfo', 'getEpochSchedule',
            'getFeeForMessage', 'getFirstAvailableBlock', 'getGenesisHash', 'getHealth',
            'getIdentity', 'getInflationGovernor', 'getInflationRate', 'getInflationReward',
            'getLargestAccounts', 'getLatestBlockhash', 'getLeaderSchedule', 'getMaxRetransmitSlot',
            'getMaxShredInsertSlot', 'getMinimumBalanceForRentExemption', 'getMultipleAccounts',
            'getProgramAccounts', 'getRecentPerformanceSamples', 'getSignaturesForAddress',
            'getSignatureStatuses', 'getSlot', 'getSlotLeader', 'getSlotLeaders',
            'getStakeActivation', 'getStakeMinimumDelegation', 'getSupply', 'getTokenAccountBalance',
            'getTokenAccountsByDelegate', 'getTokenAccountsByOwner', 'getTokenLargestAccounts',
            'getTokenSupply', 'getTransaction', 'getTransactionCount', 'getVersion',
            'getVoteAccounts', 'isBlockhashValid', 'requestAirdrop', 'sendTransaction',
            // Added methods often used with sendTransaction
             'confirmTransaction', 'getParsedTransaction', 'getParsedTransactions'
        ];

        // Get all methods from the Connection prototype
        const allProtoMethods = Object.getOwnPropertyNames(Connection.prototype)
            .filter(prop => typeof Connection.prototype[prop] === 'function' && prop !== 'constructor');

        // Combine explicitly listed methods with all prototype methods for broader coverage
        const uniqueMethods = [...new Set([...rpcMethods, ...allProtoMethods])];

        uniqueMethods.forEach(method => {
            // Ensure we don't overwrite essential non-RPC methods or the constructor
            if (typeof super[method] === 'function' && method !== 'constructor') {
                this[method] = (...args) => {
                    // Don't wrap internal methods starting with '_'
                    if (method.startsWith('_')) {
                        // If an internal method needs rate limiting/retries, it needs specific handling
                        // For now, call the original directly.
                        // console.warn(`Calling internal method directly: ${method}`); // Optional warning
                        return super[method](...args);
                    }
                    return this._enqueueRequest(method, args);
                };
            }
        });

        // <<< REMOVED the problematic line: this._rpcRequest = super._rpcRequest.bind(this); >>>
    }

    async _enqueueRequest(method, args) {
        this.stats.totalRequestsEnqueued++;
        return new Promise((resolve, reject) => {
            const request = {
                method,
                args,
                resolve,
                reject,
                retries: 0,
                timestamp: Date.now() // For potential timeout logic later
            };

            // Simple priority: Add requests to the front if we are currently rate-limited
            if (this.consecutiveRateLimits > 0) {
                this.requestQueue.unshift(request); // Prioritize if recently rate limited
            } else {
                this.requestQueue.push(request); // Normal FIFO
            }

            // Use setImmediate to avoid blocking the event loop if queue processing is synchronous
            setImmediate(() => this._processQueue());
        });
    }

    async _processQueue() {
        // Check conditions preventing processing
        if (this.activeRequests >= this.options.maxConcurrent) {
            return;
        }
        if (this.requestQueue.length === 0) {
            return;
        }
        const timeSinceLastRateLimit = Date.now() - this.lastRateLimitTime;
        if (timeSinceLastRateLimit < this.options.rateLimitCooloff) {
             // Schedule a check after the cooloff period ends
             // Avoid scheduling multiple timeouts if already cooling off
             if (!this.cooloffTimeout) {
                this.cooloffTimeout = setTimeout(() => {
                    this.cooloffTimeout = null; // Clear the timeout handle
                    this._processQueue();
                }, this.options.rateLimitCooloff - timeSinceLastRateLimit + 50); // +50ms buffer
             }
            return;
        }

        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            // --- Execute the actual RPC call ---
             const jitter = Math.random() * 200; // 0-200ms
             await new Promise(r => setTimeout(r, jitter));

             const result = await super[request.method](...request.args);
            // --- Success ---
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            // Reset consecutive rate limit counter on success
            this.consecutiveRateLimits = 0;

        } catch (error) {
            // --- Error Handling & Retry Logic ---
            console.warn(`Error in ${request.method} (Attempt ${request.retries + 1}):`, error.message); // Log warning on error

            if (this._shouldRetry(error, request.retries)) {
                 // --- Retry ---
                request.retries++;
                const delay = this._calculateRetryDelay(request.retries);
                console.log(`Retrying ${request.method} after ${delay.toFixed(0)}ms delay (Retry ${request.retries}/${this.options.maxRetries}). Consecutive Fails: ${this.consecutiveRateLimits}`);

                try {
                    await new Promise(resolve => setTimeout(resolve, delay));
                    // Requeue the request for another attempt
                    this._requeueRequest(request);
                } catch (requeueError) {
                     console.error(`Error during requeue wait/logic for ${request.method}:`, requeueError);
                     this.stats.totalRequestsFailed++;
                     request.reject(error); // Reject with the original error if requeue fails
                }

            } else {
                 // --- Do Not Retry: Final Failure ---
                console.error(`Request ${request.method} failed permanently after ${request.retries} retries. Error: ${error.message}`);
                this.stats.totalRequestsFailed++;
                request.reject(error); // Reject the promise with the final error
                 // Reset consecutive fails on permanent failure of a request
                 this.consecutiveRateLimits = 0;
            }
        } finally {
            // --- Cleanup ---
            this.activeRequests--;
            // Trigger processing the next item immediately after one finishes
            setImmediate(() => this._processQueue());
        }
    }

     /**
     * Calculates the delay for the next retry attempt using exponential backoff and jitter.
     * @param {number} retryCount - The number of retries already attempted (0-indexed).
     * @returns {number} Delay in milliseconds.
     */
    _calculateRetryDelay(retryCount) {
        const exponentialDelay = this.options.retryBaseDelay * Math.pow(2, retryCount);
        const cappedDelay = Math.min(exponentialDelay, this.options.retryMaxDelay);
        const jitterMagnitude = cappedDelay * this.options.retryJitter;
        const jitter = (Math.random() * 2 - 1) * jitterMagnitude;
        return Math.max(0, cappedDelay + jitter);
    }

    /**
     * Determines if a request should be retried based on the error and retry count.
     * @param {Error} error - The error received.
     * @param {number} retryCount - The number of retries already attempted.
     * @returns {boolean} True if the request should be retried, false otherwise.
     */
    _shouldRetry(error, retryCount) {
        if (retryCount >= this.options.maxRetries) {
            return false;
        }

        const errorMessage = error?.message?.toLowerCase() || '';
        const errorCode = error?.code?.toLowerCase();
        const errorStatus = error?.response?.status || error?.statusCode;

        // Check for Rate Limit (429)
        if (errorStatus === 429 || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
            this.lastRateLimitTime = Date.now();
            this.consecutiveRateLimits++;
            this.stats.rateLimitEvents++;
             console.warn(`Rate limit detected! Consecutive: ${this.consecutiveRateLimits}. Cooling off for ${this.options.rateLimitCooloff}ms.`);
            return true;
        }

        // Check for common retryable network/server errors
        const retryableMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated'];
        const retryableCodes = ['etimedout', 'econnreset', 'enetunreach', 'eai_again', 'econnaborted'];

        if (retryableMessages.some(msg => errorMessage.includes(msg)) || (errorCode && retryableCodes.includes(errorCode))) {
             this.consecutiveRateLimits = 0;
            return true;
        }

         // Handle potential Solana JSON RPC error structure for specific retryable server errors (e.g., 503)
         if (errorStatus === 503 || errorStatus === 504 ) { // Service Unavailable, Gateway Timeout
              this.consecutiveRateLimits = 0;
              return true;
         }

         this.consecutiveRateLimits = 0;
        return false;
    }

    /**
     * Re-adds a request to the queue, potentially with adjusted priority based on retries.
     * @param {object} request - The request object to requeue.
     */
    _requeueRequest(request) {
        // Simple requeue: Add to the front for immediate reprocessing after delay
        this.requestQueue.unshift(request);
    }

    // --- Utility Methods ---

    /**
     * Gets current statistics about the connection and queue.
     * @returns {object} Connection statistics.
     */
    getRequestStats() {
        const successRate = this.stats.totalRequestsEnqueued > 0
            ? ((this.stats.totalRequestsSucceeded) / this.stats.totalRequestsEnqueued * 100).toFixed(2)
            : 'N/A';

        return {
            options: this.options,
            status: {
                queueSize: this.requestQueue.length,
                activeRequests: this.activeRequests,
                consecutiveRateLimits: this.consecutiveRateLimits,
                lastRateLimit: this.lastRateLimitTime > 0
                    ? `${Math.floor((Date.now() - this.lastRateLimitTime) / 1000)}s ago`
                    : 'never'
            },
            stats: {
                 ...this.stats,
                 successRate: `${successRate}%`
            }
        };
    }

    /**
    * Resets rate limit counters externally if needed.
    */
    resetRateLimitCounters() {
        console.log("Resetting rate limit counters externally.");
        this.consecutiveRateLimits = 0;
        this.lastRateLimitTime = 0;
    }
}

export default RateLimitedConnection;
