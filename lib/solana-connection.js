import { Connection } from '@solana/web3.js';

class RateLimitedConnection extends Connection {
    constructor(endpoint, options = {}) {
        super(endpoint, {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': `SolanaGamblesBot/2.0 (${options.clientId || 'default'})`,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined,
            disableRetryOnRateLimit: false // Our custom option
        });

        // Enhanced rate limiting configuration
        this.maxConcurrent = options.maxConcurrent || 5;
        this.retryBaseDelay = options.retryBaseDelay || 600; // More conservative default
        this.maxRetries = options.maxRetries || 5;
        this.rateLimitCooloff = options.rateLimitCooloff || 5000; // 5s cooloff after 429
        
        // Advanced queue management
        this.requestQueue = [];
        this.activeRequests = 0;
        this.totalRequests = 0;
        this.failedRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;

        // Bind all RPC methods
        this._wrapRpcMethods();
    }

    _wrapRpcMethods() {
        const rpcMethods = [
            'getAccountInfo',
            'getBalance',
            'getBlock',
            'getBlockHeight',
            'getBlockProduction',
            'getBlockCommitment',
            'getBlocks',
            'getBlocksWithLimit',
            'getBlockTime',
            'getClusterNodes',
            'getEpochInfo',
            'getEpochSchedule',
            'getFeeForMessage',
            'getFirstAvailableBlock',
            'getGenesisHash',
            'getHealth',
            'getIdentity',
            'getInflationGovernor',
            'getInflationRate',
            'getInflationReward',
            'getLargestAccounts',
            'getLatestBlockhash',
            'getLeaderSchedule',
            'getMaxRetransmitSlot',
            'getMaxShredInsertSlot',
            'getMinimumBalanceForRentExemption',
            'getMultipleAccounts',
            'getProgramAccounts',
            'getRecentPerformanceSamples',
            'getSignaturesForAddress',
            'getSignatureStatuses',
            'getSlot',
            'getSlotLeader',
            'getSlotLeaders',
            'getStakeActivation',
            'getStakeMinimumDelegation',
            'getSupply',
            'getTokenAccountBalance',
            'getTokenAccountsByDelegate',
            'getTokenAccountsByOwner',
            'getTokenLargestAccounts',
            'getTokenSupply',
            'getTransaction',
            'getTransactionCount',
            'getVersion',
            'getVoteAccounts',
            'isBlockhashValid',
            'requestAirdrop',
            'sendTransaction'
        ];

        rpcMethods.forEach(method => {
            this[method] = async (...args) => {
                return this._enqueueRequest(method, args);
            };
        });
    }

    async _enqueueRequest(method, args) {
        this.totalRequests++;
        
        return new Promise((resolve, reject) => {
            const request = {
                method,
                args,
                resolve,
                reject,
                retries: 0,
                timestamp: Date.now()
            };
            
            // Prioritize requests after rate limits
            if (this.consecutiveRateLimits > 2) {
                this.requestQueue.unshift(request); // Add to front
            } else {
                this.requestQueue.push(request); // Normal queue
            }
            
            this._processQueue();
        });
    }

    async _processQueue() {
        // Don't process if:
        // - At max concurrent requests
        // - Queue is empty
        // - In rate limit cooloff period
        if (this.activeRequests >= this.maxConcurrent || 
            this.requestQueue.length === 0 ||
            Date.now() - this.lastRateLimitTime < this.rateLimitCooloff) {
            return;
        }

        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            const result = await this._executeWithRetry(request);
            request.resolve(result);
        } catch (error) {
            if (this._shouldRetry(error, request.retries)) {
                request.retries++;
                this._requeueRequest(request);
            } else {
                this.failedRequests++;
                request.reject(error);
            }
        } finally {
            this.activeRequests--;
            process.nextTick(() => this._processQueue()); // Prevent stack overflow
        }
    }

    async _executeWithRetry(request) {
        try {
            // Add slight jitter to avoid synchronized retries
            const jitter = Math.random() * 200; // 0-200ms
            await new Promise(r => setTimeout(r, jitter));
            
            return await super[request.method](...request.args);
        } catch (error) {
            if (this._shouldRetry(error, request.retries)) {
                const delay = this._calculateRetryDelay(request.retries);
                await new Promise(r => setTimeout(r, delay));
                return this._executeWithRetry({
                    ...request,
                    retries: request.retries + 1
                });
            }
            throw error;
        }
    }

    _calculateRetryDelay(retryCount) {
        // Exponential backoff with jitter
        const baseDelay = Math.min(
            this.retryBaseDelay * Math.pow(2, retryCount),
            30000 // Max 30s delay
        );
        return baseDelay * (0.8 + Math.random() * 0.4); // 20% jitter
    }

    _shouldRetry(error, retryCount) {
        if (retryCount >= this.maxRetries) return false;
        
        // Detect rate limits
        if (error.message?.includes?.('429') || 
            error.message?.includes?.('rate limit') ||
            error.message?.includes?.('Too Many Requests') ||
            error.response?.status === 429) {
            
            this.lastRateLimitTime = Date.now();
            this.consecutiveRateLimits++;
            return true;
        }

        // Network errors
        return [
            'ETIMEDOUT',
            'ECONNRESET', 
            'ENETUNREACH',
            'EAI_AGAIN',
            'socket hang up'
        ].some(msg => error.message?.includes?.(msg));
    }

    _requeueRequest(request) {
        // Exponential priority boost for retries
        const priorityBoost = Math.pow(2, request.retries);
        const insertPosition = Math.min(
            priorityBoost, 
            this.requestQueue.length
        );
        this.requestQueue.splice(insertPosition, 0, request);
    }

    getRequestStats() {
        return {
            total: this.totalRequests,
            failed: this.failedRequests,
            successRate: this.totalRequests > 0 
                ? ((this.totalRequests - this.failedRequests) / this.totalRequests * 100).toFixed(2)
                : 100,
            queueSize: this.requestQueue.length,
            active: this.activeRequests,
            consecutiveRateLimits: this.consecutiveRateLimits,
            lastRateLimit: this.lastRateLimitTime > 0 
                ? Math.floor((Date.now() - this.lastRateLimitTime)/1000) + 's ago' 
                : 'never'
        };
    }

    // New method to reset rate limit counters
    resetRateLimitCounters() {
        this.consecutiveRateLimits = 0;
        this.lastRateLimitTime = 0;
    }
}

export default RateLimitedConnection;
