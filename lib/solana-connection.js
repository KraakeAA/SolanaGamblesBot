// lib/solana-connection.js

import { Connection } from '@solana/web3.js';

class RateLimitedConnection extends Connection {
    /**
     * Creates a rate-limited connection that rotates through multiple RPC endpoints.
     * @param {string[]} endpoints - An array of RPC endpoint URLs.
     * @param {object} options - Configuration options.
     * @param {string} [options.commitment='confirmed'] - Default commitment level.
     * @param {object} [options.httpHeaders={}] - Additional HTTP headers.
     * @param {string} [options.wsEndpoint] - Optional WebSocket endpoint (uses the one corresponding to the current HTTP endpoint if not specified).
     * @param {string} [options.clientId='multi-rpc-connection'] - Client identifier for headers.
     * @param {number} [options.maxConcurrent=5] - Max parallel requests.
     * @param {number} [options.retryBaseDelay=600] - Initial delay for retries (ms).
     * @param {number} [options.maxRetries=5] - Max attempts per request (per endpoint cycle).
     * @param {number} [options.rateLimitCooloff=5000] - Min pause duration after hitting 429 (ms).
     * @param {number} [options.retryMaxDelay=30000] - Max delay for exponential backoff (ms).
     * @param {number} [options.retryJitter=0.2] - Percentage jitter for retry delay (0.2 = +/- 20%).
     */
    constructor(endpoints, options = {}) {
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        // --- Multi-RPC State ---
        this.endpoints = [...endpoints]; // Store all endpoints
        this.currentEndpointIndex = 0;   // Start with the first endpoint

        const initialEndpoint = this.endpoints[this.currentEndpointIndex];
        console.log(`[RLC] Initializing with endpoint: ${initialEndpoint}`);

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': `SolanaGamblesBot/2.1-multi-rpc (${options.clientId || 'multi-rpc-connection'})`,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined, // Allow explicit WS endpoint
            disableRetryOnRateLimit: false // We handle retries/rate limits ourselves
        };
        super(initialEndpoint, rpcOptions);

        // --- Configuration ---
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5, // Total retries allowed for a request
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2
        };

        // --- State ---
        this.requestQueue = [];         // Holds { method, args, resolve, reject, retries, originalEndpointIndex }
        this.activeRequests = 0;        // Current number of in-flight requests
        this.lastRateLimitTime = 0;     // Timestamp of the last 429 error (per endpoint potentially, but simplified here)
        this.consecutiveRateLimits = 0; // Counter for consecutive 429s
        this.cooloffTimeout = null;     // Timeout handle for rate limit cooloff period

        // --- Statistics ---
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };

        // --- Initialization ---
        this._wrapRpcMethods();
        console.log("[RLC] RateLimitedConnection (Multi-RPC) initialized with options:", this.options);
        console.log(`[RLC] Endpoints configured: ${this.endpoints.join(', ')}`);
    }

    // --- Multi-RPC Methods ---

    /**
     * Gets the currently active RPC endpoint URL.
     * @returns {string} The current endpoint URL.
     */
    getCurrentEndpoint() {
        return this.endpoints[this.currentEndpointIndex];
    }

    /**
     * Gets the number of configured RPC endpoints.
     * @returns {number} The number of endpoints.
     */
    getEndpointCount() {
        return this.endpoints.length;
    }

    /**
     * Rotates to the next available RPC endpoint.
     * Updates the internal endpoint used by the Connection.
     * @returns {string} The new current endpoint URL.
     */
    _rotateEndpoint() {
        if (this.endpoints.length <= 1) {
            console.warn("[RLC] Rotation requested, but only one endpoint is configured.");
            return this.getCurrentEndpoint();
        }

        const oldIndex = this.currentEndpointIndex;
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
        const newEndpoint = this.endpoints[this.currentEndpointIndex];

        // **Critical Step:** Update the endpoint the parent Connection uses.
        // We update the public property. Testing is needed to confirm if internal
        // mechanisms (_rpcEndpoint, _rpcClient) in web3.js respect this change
        // immediately for subsequent calls via `super`. If not, a more complex
        // approach (like re-instantiating the internal client) might be required.
        this.rpcEndpoint = newEndpoint;
        // Also update the WebSocket endpoint if it wasn't explicitly set
        if (!this.options.wsEndpoint && this._wsEndpoint) {
             // Attempt to derive the WS endpoint from the new HTTP endpoint
             try {
                const url = new URL(newEndpoint);
                url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
                this._wsEndpoint = url.toString();
                console.log(`[RLC] Updated internal WebSocket endpoint to: ${this._wsEndpoint}`);
             } catch(e) {
                console.error(`[RLC] Failed to derive WebSocket endpoint from ${newEndpoint}`, e);
                // Keep the old one or set to undefined? Let's keep the old one for now.
             }
        }


        this.stats.endpointRotations++;
        console.log(`[RLC] Rotated RPC endpoint from ${this.endpoints[oldIndex]} (Index ${oldIndex}) to ${newEndpoint} (Index ${this.currentEndpointIndex}). Rotations: ${this.stats.endpointRotations}`);

        // Reset rate limit counters when rotating, assuming the limit was endpoint-specific
        this.resetRateLimitCounters();

        return newEndpoint;
    }

    // --- Overridden Methods ---

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
             // Consider adding methods related to Address Lookup Tables if used
        ];

        // Get all methods from the Connection prototype
        const allProtoMethods = Object.getOwnPropertyNames(Connection.prototype)
            .filter(prop => typeof Connection.prototype[prop] === 'function' && prop !== 'constructor');

        // Combine explicitly listed methods with all prototype methods for broader coverage
        const uniqueMethods = [...new Set([...rpcMethods, ...allProtoMethods])];


        uniqueMethods.forEach(method => {
            // Ensure we don't overwrite essential non-RPC methods or the constructor
             if (typeof super[method] === 'function' && method !== 'constructor') {
                 // Store original method reference if needed, though super[method] should work
                 // const originalMethod = super[method];

                 this[method] = (...args) => {
                     // Don't wrap internal methods starting with '_' unless necessary
                     if (method.startsWith('_')) {
                          // console.warn(`[RLC] Calling internal method directly (not rate-limited): ${method}`);
                          return super[method](...args);
                     }
                     return this._enqueueRequest(method, args);
                 };
             }
        });
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
                timestamp: Date.now(), // For potential timeout logic later
                originalEndpointIndex: this.currentEndpointIndex // Track starting endpoint for this request cycle
            };

            // Add request to the queue (FIFO for simplicity now)
            this.requestQueue.push(request);

            // Use setImmediate to avoid blocking the event loop if queue processing is synchronous
            setImmediate(() => this._processQueue());
        });
    }

    async _processQueue() {
        // Check conditions preventing processing
        if (this.activeRequests >= this.options.maxConcurrent) {
            return; // Max concurrency reached
        }
        if (this.requestQueue.length === 0) {
            return; // Queue empty
        }

        // Check rate limit cooloff ONLY if consecutive rate limits > 0
        if (this.consecutiveRateLimits > 0) {
             const timeSinceLastRateLimit = Date.now() - this.lastRateLimitTime;
             if (timeSinceLastRateLimit < this.options.rateLimitCooloff) {
                  // Schedule a check after the cooloff period ends
                  // Avoid scheduling multiple timeouts if already cooling off
                  if (!this.cooloffTimeout) {
                       // console.log(`[RLC] Cooling off for ${this.options.rateLimitCooloff - timeSinceLastRateLimit}ms more...`);
                       this.cooloffTimeout = setTimeout(() => {
                            this.cooloffTimeout = null; // Clear the timeout handle
                            this._processQueue();
                       }, this.options.rateLimitCooloff - timeSinceLastRateLimit + 50); // +50ms buffer
                  }
                  return; // Still cooling off
             } else {
                 // Cooloff finished, reset counter before proceeding
                 // console.log("[RLC] Rate limit cooloff finished.");
                 // this.resetRateLimitCounters(); // Reset counters now that cooloff is over
                 // Don't reset here - reset on success or rotation
             }
        }


        this.activeRequests++;
        const request = this.requestQueue.shift();

        // Ensure the connection is using the endpoint intended for this attempt
        // This is important if rotation happened while the request was queued.
        // We trust that _rotateEndpoint updated `this.rpcEndpoint` correctly.
        if (this.getCurrentEndpoint() !== this.rpcEndpoint) {
            console.warn(`[RLC] Mismatch between rpcManager (${this.getCurrentEndpoint()}) and connection (${this.rpcEndpoint}). Attempting request with connection's current endpoint.`);
            // This indicates a potential issue with how the endpoint is updated/read by `super` calls.
        }


        try {
            // --- Execute the actual RPC call using the parent Connection method ---
             // Add minor jitter before the request
             const jitter = Math.random() * 100; // 0-100ms jitter
             await new Promise(r => setTimeout(r, jitter));

            // console.log(`[RLC] Executing: ${request.method} on ${this.rpcEndpoint}`); // Verbose log
            const result = await super[request.method](...request.args); // Calls the original Connection method

            // --- Success ---
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            // Reset consecutive rate limit counter on success
            this.consecutiveRateLimits = 0;

        } catch (error) {
            // --- Error Handling & Retry Logic ---
            console.warn(`[RLC] Error in ${request.method} on ${this.rpcEndpoint} (Attempt ${request.retries + 1}):`, error.message); // Log warning on error

            const shouldRetry = this._shouldRetry(error, request.retries);
            const isRpcError = this._isRpcSpecificError(error); // Check if error is likely RPC endpoint specific

            if (shouldRetry) {
                 // --- Retry ---
                 request.retries++;

                 // Rotate Endpoint if it's an RPC-specific error and we have alternatives
                 if (isRpcError && this.endpoints.length > 1) {
                     this._rotateEndpoint();
                     // When rotating, we might want to reset the retry count for this request
                     // to give the new endpoint a fair chance, or keep the total count?
                     // Let's keep the total retry count across endpoints for now.
                 }

                 const delay = this._calculateRetryDelay(request.retries);
                 console.log(`[RLC] Retrying ${request.method} after ${delay.toFixed(0)}ms delay (Retry ${request.retries}/${this.options.maxRetries}). ${isRpcError ? `Rotated to: ${this.getCurrentEndpoint()}.` : ''}`);

                 try {
                     await new Promise(resolve => setTimeout(resolve, delay));
                     // Requeue the request (add to front for faster retry)
                     this._requeueRequest(request);
                 } catch (requeueError) {
                     console.error(`[RLC] Error during requeue wait/logic for ${request.method}:`, requeueError);
                     this.stats.totalRequestsFailed++;
                     request.reject(error); // Reject with the original error if requeue fails
                 }

            } else {
                 // --- Do Not Retry: Final Failure ---
                 console.error(`[RLC] Request ${request.method} failed permanently after ${request.retries} retries on endpoint ${this.rpcEndpoint}. Error: ${error.message}`);
                 this.stats.totalRequestsFailed++;
                 request.reject(error); // Reject the promise with the final error
                 // Reset consecutive fails on permanent failure of a request
                 // this.consecutiveRateLimits = 0; // Keep it if it was a rate limit that caused final failure
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
        // Factor in consecutive rate limits for potentially longer delays
        const backoffFactor = Math.max(1, Math.pow(2, this.consecutiveRateLimits));
        const exponentialDelay = this.options.retryBaseDelay * Math.pow(2, retryCount) * backoffFactor;

        const cappedDelay = Math.min(exponentialDelay, this.options.retryMaxDelay);
        const jitterMagnitude = cappedDelay * this.options.retryJitter;
        const jitter = (Math.random() * 2 - 1) * jitterMagnitude; // -jitterMagnitude to +jitterMagnitude
        const finalDelay = Math.max(0, cappedDelay + jitter);

        // If we were rate limited, ensure minimum cooloff period is respected
        if (this.consecutiveRateLimits > 0) {
            return Math.max(finalDelay, this.options.rateLimitCooloff);
        }

        return finalDelay;
    }

     /**
      * Determines if a request should be retried based on the error and retry count.
      * @param {Error | any} error - The error received.
      * @param {number} retryCount - The number of retries already attempted.
      * @returns {boolean} True if the request should be retried, false otherwise.
      */
    _shouldRetry(error, retryCount) {
        if (retryCount >= this.options.maxRetries) {
            return false; // Max retries exceeded
        }

        const errorMessage = error?.message?.toLowerCase() || '';
        const errorStatus = error?.response?.status || error?.statusCode || error?.code; // Broader check for status/code

        // Check for Rate Limit (429)
        if (errorStatus === 429 || errorStatus === '429' || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
            if (this.lastRateLimitTime !== 0 && (Date.now() - this.lastRateLimitTime) < this.options.rateLimitCooloff) {
                // Avoid incrementing consecutive counter if still within the cooloff window from the *last* 429 event
            } else {
                 this.consecutiveRateLimits++;
            }
            this.lastRateLimitTime = Date.now();
            this.stats.rateLimitEvents++;
            console.warn(`[RLC] Rate limit detected! Consecutive: ${this.consecutiveRateLimits}. Will cool off.`);
            return true;
        }

        // Check for common retryable network/server errors (Codes or Status)
        const retryableStatuses = [500, 502, 503, 504]; // Internal Server Error, Bad Gateway, Service Unavailable, Gateway Timeout
        if (retryableStatuses.includes(Number(errorStatus))) {
             // Reset consecutive rate limits if it's a different server error
             this.consecutiveRateLimits = 0;
             return true;
        }

        // Check common retryable network error messages/codes
        const retryableMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'transaction simulation failed', 'failed to simulate'];
        const retryableCodes = ['etimedout', 'econnreset', 'enetunreach', 'eai_again', 'econnaborted', 'econnrefused']; // Node.js specific codes

        if (retryableMessages.some(msg => errorMessage.includes(msg)) || (error?.code && retryableCodes.includes(error.code.toLowerCase()))) {
            // Reset consecutive rate limits if it's a different network error
             this.consecutiveRateLimits = 0;
             return true;
        }

        // If none of the above, assume not retryable
        // Reset consecutive rate limit counter if the error wasn't a 429
        // this.consecutiveRateLimits = 0; // Resetting here might be too broad, only reset on success or rotation
        return false;
    }

     /**
      * Checks if an error is likely specific to the RPC endpoint (and warrants rotation).
      * @param {Error | any} error - The error received.
      * @returns {boolean} True if the error likely warrants RPC rotation.
      */
     _isRpcSpecificError(error) {
        const errorMessage = error?.message?.toLowerCase() || '';
        const errorStatus = error?.response?.status || error?.statusCode || error?.code;

        // Rate limits are endpoint specific
        if (errorStatus === 429 || errorStatus === '429' || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
            return true;
        }
        // Server errors (5xx) are endpoint specific
        if ([500, 502, 503, 504].includes(Number(errorStatus))) {
             return true;
        }
        // Connection timeouts / refusals are endpoint specific
        if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'econnrefused', 'failed to fetch', 'socket hang up', 'connection refused', 'etimedout', 'econnreset', 'econnaborted'].some(msg => errorMessage.includes(msg) || error?.code?.toLowerCase() === msg)) {
             return true;
        }
        // Simulation failures can sometimes be endpoint specific load issues
        if (errorMessage.includes('transaction simulation failed') || errorMessage.includes('failed to simulate')) {
             return true;
        }

        return false; // Assume other errors (like bad request, blockhash not found) are not endpoint-specific
     }

    /**
     * Re-adds a request to the queue, potentially with adjusted priority based on retries.
     * @param {object} request - The request object to requeue.
     */
    _requeueRequest(request) {
        // Requeue at the front for immediate reprocessing after delay
        this.requestQueue.unshift(request);
    }

    // --- Utility Methods ---

    /**
     * Gets current statistics about the connection and queue.
     * @returns {object} Connection statistics.
     */
    getRequestStats() {
        const successRate = (this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed) > 0
            ? ((this.stats.totalRequestsSucceeded) / (this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed) * 100).toFixed(2)
            : 'N/A';

        return {
            options: this.options,
            status: {
                queueSize: this.requestQueue.length,
                activeRequests: this.activeRequests,
                currentEndpoint: this.getCurrentEndpoint(),
                currentEndpointIndex: this.currentEndpointIndex,
                consecutiveRateLimits: this.consecutiveRateLimits,
                lastRateLimit: this.lastRateLimitTime > 0
                    ? `${Math.floor((Date.now() - this.lastRateLimitTime) / 1000)}s ago`
                    : 'never'
            },
            stats: {
                 ...this.stats,
                 successRate: `${successRate}%` // Success rate based on completed requests
            }
        };
    }

    /**
    * Resets rate limit counters externally if needed.
    */
    resetRateLimitCounters() {
        // console.log("[RLC] Resetting rate limit counters."); // Reduce noise
        this.consecutiveRateLimits = 0;
        this.lastRateLimitTime = 0;
        // Clear cooloff timeout if it exists
        if (this.cooloffTimeout) {
            clearTimeout(this.cooloffTimeout);
            this.cooloffTimeout = null;
        }
    }
}

export default RateLimitedConnection;
