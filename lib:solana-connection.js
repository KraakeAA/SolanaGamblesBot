const { Connection } = require('@solana/web3.js');

class RateLimitedConnection extends Connection {
    constructor(endpoint, options = {}) {
        super(endpoint, {
            ...options,
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': 'SolanaGamblesBot/1.0',
                ...(options.httpHeaders || {})
            }
        });

        // Rate limiting configuration
        this.maxConcurrent = options.maxConcurrent || 3;
        this.retryBaseDelay = options.retryBaseDelay || 500;
        this.maxRetries = options.maxRetries || 3;
        
        // Queue management
        this.requestQueue = [];
        this.activeRequests = 0;
        this.totalRequests = 0;
        this.failedRequests = 0;

        // Bind all RPC methods to enable queueing
        this._wrapRpcMethods();
    }

    _wrapRpcMethods() {
        // List of all RPC methods we want to rate limit
        const rpcMethods = [
            'getAccountInfo',
            'getBalance',
            'getParsedTransaction',
            'getSignaturesForAddress',
            'sendTransaction',
            'getLatestBlockhash',
            'getTransaction',
            'getSlot'
        ];

        // Wrap each method with our queue system
        rpcMethods.forEach(method => {
            this[method] = async (...args) => {
                return this._enqueueRequest(method, args);
            };
        });
    }

    async _enqueueRequest(method, args) {
        this.totalRequests++;
        
        return new Promise((resolve, reject) => {
            this.requestQueue.push({
                method,
                args,
                resolve,
                reject,
                retries: 0
            });
            this._processQueue();
        });
    }

    async _processQueue() {
        if (this.activeRequests >= this.maxConcurrent || this.requestQueue.length === 0) {
            return;
        }

        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            const result = await this._executeWithRetry(
                request.method, 
                request.args, 
                request.retries
            );
            request.resolve(result);
        } catch (error) {
            if (error.message.includes('429') && request.retries < this.maxRetries) {
                // Requeue if rate limited
                request.retries++;
                this.requestQueue.unshift(request);
                
                // Exponential backoff
                const delay = this.retryBaseDelay * Math.pow(2, request.retries - 1);
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                this.failedRequests++;
                request.reject(error);
            }
        } finally {
            this.activeRequests--;
            this._processQueue();
        }
    }

    async _executeWithRetry(method, args, attempt = 0) {
        try {
            // Use the original Connection method
            const result = await super[method](...args);
            return result;
        } catch (error) {
            if (attempt < this.maxRetries && this._isRetryableError(error)) {
                const delay = this.retryBaseDelay * Math.pow(2, attempt);
                await new Promise(resolve => setTimeout(resolve, delay));
                return this._executeWithRetry(method, args, attempt + 1);
            }
            throw error;
        }
    }

    _isRetryableError(error) {
        const retryableMessages = [
            '429',
            'rate limit',
            'too many requests',
            'timeout',
            'connection reset',
            'socket hang up'
        ];
        
        return retryableMessages.some(msg => 
            error.message.includes(msg)
        );
    }

    getRequestStats() {
        return {
            total: this.totalRequests,
            failed: this.failedRequests,
            successRate: ((this.totalRequests - this.failedRequests) / this.totalRequests * 100).toFixed(2),
            queueSize: this.requestQueue.length,
            active: this.activeRequests
        };
    }
}

module.exports = { RateLimitedConnection };