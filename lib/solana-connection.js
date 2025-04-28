
// lib/solana-connection.js (UPDATED with full logging)

import { Connection } from '@solana/web3.js';

let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

class RateLimitedConnection extends Connection {
    constructor(endpoints, options = {}) {
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        const initialEndpoint = endpoints[0];
        const clientId = options.clientId || `SolanaGamblesBot/2.1-multi-rpc (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined,
            disableRetryOnRateLimit: true
        };

        console.log(`[RLC] Connecting to initial RPC endpoint: ${initialEndpoint}`);
        super(initialEndpoint, rpcOptions);

        this.endpoints = [...endpoints];
        this.currentEndpointIndex = 0;

        this.baseHttpHeaders = {
            'Content-Type': 'application/json',
            'solana-client': clientId,
            ...(options.httpHeaders || {})
        };

        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5,
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2,
        };

        this.requestQueue = [];
        this.activeRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;
        this.cooloffTimeout = null;

        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };
    }

    getCurrentEndpoint() {
        if (this.currentEndpointIndex < 0 || this.currentEndpointIndex >= this.endpoints.length) {
            console.warn(`[RLC] Invalid currentEndpointIndex (${this.currentEndpointIndex}), resetting.`);
            this.currentEndpointIndex = 0;
        }
        return this.endpoints[this.currentEndpointIndex];
    }

    _rotateEndpoint() {
        if (this.endpoints.length <= 1) return this.getCurrentEndpoint();

        const oldIndex = this.currentEndpointIndex;
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
        this.stats.endpointRotations++;
        console.log(`[RLC] Rotated RPC from index ${oldIndex} to ${this.currentEndpointIndex}: ${this.getCurrentEndpoint()}`);
        this.consecutiveRateLimits = 0;
        return this.getCurrentEndpoint();
    }

    async _rpcRequest(method, args) {
        return this._enqueueRpcOperation(method, args);
    }

    _enqueueRpcOperation(method, args) {
        this.stats.totalRequestsEnqueued++;
        return new Promise((resolve, reject) => {
            const request = {
                method,
                args,
                resolve,
                reject,
                retries: 0,
                timestamp: Date.now(),
                originalEndpointIndex: this.currentEndpointIndex
            };
            this.requestQueue.push(request);
            setImmediate(() => this._processQueue());
        });
    }

    async _processQueue() {
        if (this.activeRequests >= this.options.maxConcurrent) return;
        if (this.requestQueue.length === 0) return;

        if (this.consecutiveRateLimits > 0) {
            const timeSinceLast = Date.now() - this.lastRateLimitTime;
            const cooloff = Math.min(this.options.rateLimitCooloff * (2 ** (this.consecutiveRateLimits - 1)), this.options.retryMaxDelay);
            if (timeSinceLast < cooloff) {
                if (!this.cooloffTimeout) {
                    this.cooloffTimeout = setTimeout(() => {
                        this.cooloffTimeout = null;
                        this._processQueue();
                    }, cooloff - timeSinceLast + 50);
                }
                return;
            }
        }

        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            const result = await this._doRpcRequest(request.method, request.args);
            console.log(`[RLC] RPC Success: ${request.method} [ID ${nextRequestId - 1}]`);
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            this.consecutiveRateLimits = 0;
        } catch (error) {
            console.warn(`[RLC] RPC Error on method ${request.method}: ${error.message}`);
            const shouldRetry = request.retries < this.options.maxRetries;
            if (shouldRetry) {
                request.retries++;
                if (this.endpoints.length > 1) {
                    this._rotateEndpoint();
                }
                const delay = this._calculateRetryDelay(request.retries);
                setTimeout(() => {
                    this.requestQueue.unshift(request);
                    this._processQueue();
                }, delay);
            } else {
                this.stats.totalRequestsFailed++;
                request.reject(error);
            }
        } finally {
            this.activeRequests--;
            setImmediate(() => this._processQueue());
        }
    }

    async _doRpcRequest(method, params) {
        const currentEndpoint = this.getCurrentEndpoint();
        const requestId = getNextRequestId();
        const payload = {
            jsonrpc: '2.0',
            id: requestId,
            method,
            params
        };

        const jitter = Math.random() * 30;
        await new Promise(r => setTimeout(r, jitter));

        const response = await fetch(currentEndpoint, {
            method: 'POST',
            headers: this.baseHttpHeaders,
            body: JSON.stringify(payload),
            signal: AbortSignal.timeout(30000)
        });

        if (!response.ok) {
            const errorText = await response.text();
            const httpError = new Error(`HTTP ${response.status}: ${errorText}`);
            httpError.code = response.status;
            throw httpError;
        }

        const json = await response.json();
        if (json.error) {
            const rpcError = new Error(`RPC ${method} failed: ${json.error.message}`);
            rpcError.code = json.error.code;
            throw rpcError;
        }

        return json.result;
    }

    _calculateRetryDelay(retries) {
        const base = this.options.retryBaseDelay;
        const jitter = base * this.options.retryJitter * (Math.random() - 0.5);
        return Math.min(base * (2 ** retries) + jitter, this.options.retryMaxDelay);
    }
}

export default RateLimitedConnection;
