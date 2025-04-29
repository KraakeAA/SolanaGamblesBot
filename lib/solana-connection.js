// lib/solana-connection.js (CORRECTED constructor call order)

import { Connection } from '@solana/web3.js';

let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

// --- Define your specific endpoint details ---
const HELIUS_HTTPS_URL = 'https://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';
const HELIUS_WSS_URL = 'wss://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';

const QUICKNODE_HTTPS_URL = 'https://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';
const QUICKNODE_WSS_URL = QUICKNODE_HTTPS_URL.replace(/^https:/, 'wss:');
// --- End endpoint definitions ---


class RateLimitedConnection extends Connection {
    constructor(endpoints, options = {}) {
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        const validEndpoints = endpoints.filter(ep => ep === HELIUS_HTTPS_URL || ep === QUICKNODE_HTTPS_URL);
        if (validEndpoints.length !== endpoints.length) {
             console.warn("[RLC] Constructor Warning: Input endpoints array contains URLs other than the expected Helius and QuickNode URLs. Only using valid ones.", { input: endpoints, valid: validEndpoints });
             if (validEndpoints.length === 0) {
                 throw new Error("RateLimitedConnection: No valid Helius or QuickNode endpoints provided in the endpoints array.");
             }
             endpoints = validEndpoints;
        }

        const initialEndpoint = endpoints[0];
        const clientId = options.clientId || `SolanaGamblesBot/2.2-multi-rpc (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        // --- Determine initial WSS endpoint BEFORE calling super() ---
        // --- Call the static helper method using the Class name ---
        const initialWssEndpoint = RateLimitedConnection._getWssEndpoint(initialEndpoint);
        if (!initialWssEndpoint) {
            console.warn(`[RLC] Could not determine initial WSS endpoint for ${initialEndpoint}. WebSocket features might fail initially.`);
        }
        console.log(`[RLC] Determined initial WSS endpoint: ${initialWssEndpoint || 'None'}`);
        // --- End WSS determination ---

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: {
                'Content-Type': 'application/json',
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: initialWssEndpoint, // Pass explicit WSS endpoint
            disableRetryOnRateLimit: true,
            fetch: fetch
        };

        console.log(`[RLC] Calling super() for initial RPC endpoint: ${initialEndpoint}`);
        // --- Call super() FIRST ---
        super(initialEndpoint, rpcOptions);
        // --- Now 'this' is available ---

        // Store endpoints for rotation logic
        this.endpoints = [...endpoints];
        this.currentEndpointIndex = 0;

        // Store base headers for custom fetch calls
        this.baseHttpHeaders = {
            'Content-Type': 'application/json',
            'solana-client': clientId,
            ...(options.httpHeaders || {})
        };

        // Store options for retry/rate limit logic
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5,
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2,
        };

        // Initialize state
        this.requestQueue = [];
        this.activeRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;
        this.cooloffTimeout = null;

        // Initialize stats
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };
        console.log("[RLC] RateLimitedConnection constructed successfully."); // Add log
    }

    // --- Helper to get WSS endpoint - NOW STATIC ---
    static _getWssEndpoint(httpsUrl) {
        if (httpsUrl === HELIUS_HTTPS_URL) {
            return HELIUS_WSS_URL;
        } else if (httpsUrl === QUICKNODE_HTTPS_URL) {
            return QUICKNODE_WSS_URL;
        } else {
            console.warn(`[RLC Helper Static] Unknown HTTPS URL provided: ${httpsUrl}. Cannot determine specific WSS URL.`);
            try {
                // Fallback attempt
                return httpsUrl.replace(/^https:/, 'wss:');
            } catch (e) {
                return undefined;
            }
        }
    }
    // --- End Helper ---

    getCurrentEndpoint() {
        if (this.currentEndpointIndex < 0 || this.currentEndpointIndex >= this.endpoints.length) {
            console.warn(`[RLC] Invalid currentEndpointIndex (${this.currentEndpointIndex}), resetting to 0.`);
            this.currentEndpointIndex = 0;
        }
        return this.endpoints[this.currentEndpointIndex];
    }

     _rotateEndpoint() {
         if (this.endpoints.length <= 1) {
              console.log("[RLC] Rotation requested but only one endpoint configured.");
              return this.getCurrentEndpoint();
         }

         const oldIndex = this.currentEndpointIndex;
         this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
         const newEndpoint = this.endpoints[this.currentEndpointIndex];
         // --- Use the STATIC helper here too for consistency, or keep using 'this' (both work now) ---
         // const newWssEndpoint = this._getWssEndpoint(newEndpoint); // Works fine
         const newWssEndpoint = RateLimitedConnection._getWssEndpoint(newEndpoint); // Also works

         this.stats.endpointRotations++;
         console.log(`[RLC] Rotated RPC from index ${oldIndex} to ${this.currentEndpointIndex}: ${newEndpoint}`);

         // --- CRITICAL: Update parent Connection state ---
         this._rpcEndpoint = newEndpoint;
         this._rpcWsEndpoint = newWssEndpoint; // Set the determined WSS endpoint
         console.log(`[RLC] Updated internal Connection state: HTTP=${this._rpcEndpoint}, WSS=${this._rpcWsEndpoint || 'Not Set'}`);
         // --- End Update ---

         this.consecutiveRateLimits = 0;
         return newEndpoint;
     }

    // Override _rpcRequest to use our queuing and custom fetch logic
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
            const cooloff = Math.min(
                 this.options.rateLimitCooloff * (2 ** (this.consecutiveRateLimits - 1)),
                 this.options.retryMaxDelay
            );

            if (timeSinceLast < cooloff) {
                if (!this.cooloffTimeout) {
                    const delay = cooloff - timeSinceLast + 50;
                    // console.log(`[RLC] Rate limit cooloff active. Waiting ${delay.toFixed(0)}ms.`); // Reduce log noise
                    this.cooloffTimeout = setTimeout(() => {
                        this.cooloffTimeout = null;
                        this._processQueue();
                    }, delay);
                    if (this.cooloffTimeout.unref) this.cooloffTimeout.unref();
                }
                return;
            } else {
                // console.log(`[RLC] Rate limit cooloff period ended (${cooloff.toFixed(0)}ms). Resuming processing.`); // Reduce log noise
            }
        }

        this.activeRequests++;
        const request = this.requestQueue.shift();
        const requestId = getNextRequestId();

        try {
            const currentEndpoint = this.getCurrentEndpoint();
            const result = await this._doRpcRequest(currentEndpoint, request.method, request.args, requestId);
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            if (this.consecutiveRateLimits > 0) { // Reset only if we were rate limited before
                 // console.log("[RLC] Resetting consecutive rate limit count on success."); // Reduce log noise
                 this.consecutiveRateLimits = 0;
            }

        } catch (error) {
             // console.warn(`[RLC] RPC Attempt Failed: ${request.method} [Req ID ${requestId}] using ${this.getCurrentEndpoint()}. Error: ${error.message} (Code: ${error.code || 'N/A'})`); // Reduce log noise

            if (error.code === 429 || error.message?.includes('429')) {
                this.stats.rateLimitEvents++;
                this.lastRateLimitTime = Date.now();
                this.consecutiveRateLimits++;
                console.warn(`[RLC] Rate limit detected (Consecutive: ${this.consecutiveRateLimits}) on ${this.getCurrentEndpoint()}. Rotating endpoint and starting cooloff.`);

                this._rotateEndpoint();
                this.requestQueue.unshift(request);
                setImmediate(() => this._processQueue());

            } else {
                 const shouldRetry = request.retries < this.options.maxRetries;
                 if (shouldRetry) {
                     request.retries++;
                     // console.log(`[RLC] Retrying request ${requestId} (${request.method}). Attempt ${request.retries}/${this.options.maxRetries}. Rotating endpoint.`); // Reduce log noise

                     this._rotateEndpoint();

                     const delay = this._calculateRetryDelay(request.retries);
                     // console.log(`[RLC] Scheduling retry ${requestId} in ${delay.toFixed(0)}ms.`); // Reduce log noise
                     setTimeout(() => {
                         this.requestQueue.unshift(request);
                         this._processQueue();
                     }, delay);
                 } else {
                     console.error(`[RLC] RPC Request Failed Permanently: ${request.method} [Req ID ${requestId}] after ${request.retries} retries. Final Error: ${error.message}`);
                     this.stats.totalRequestsFailed++;
                     request.reject(error);
                     if (this.consecutiveRateLimits > 0) {
                          // console.log("[RLC] Resetting consecutive rate limit count due to different final error."); // Reduce log noise
                          this.consecutiveRateLimits = 0;
                     }
                 }
            }
        } finally {
            this.activeRequests--;
            setImmediate(() => this._processQueue());
        }
    }

    async _doRpcRequest(endpoint, method, params, requestId) {
        const payload = {
            jsonrpc: '2.0',
            id: requestId,
            method,
            params
        };

        let response;
        try {
            response = await fetch(endpoint, {
                method: 'POST',
                headers: this.baseHttpHeaders,
                body: JSON.stringify(payload),
                signal: AbortSignal.timeout(30000)
            });
        } catch (fetchError) {
            // console.warn(`[RLC] Fetch error for ${method} [Req ID ${requestId}] to ${endpoint}: ${fetchError.name} - ${fetchError.message}`); // Reduce log noise
             if (fetchError.name === 'TimeoutError' || fetchError.name === 'AbortError') {
                 fetchError.code = 'FETCH_TIMEOUT';
             } else {
                  fetchError.code = 'FETCH_ERROR';
             }
            throw fetchError;
        }

        if (!response.ok) {
             let errorText;
             try {
                 errorText = await response.text();
             } catch (e) {
                 errorText = `(Failed to read error response body: ${e.message})`;
             }
             const httpError = new Error(`HTTP ${response.status} ${response.statusText}: ${errorText.substring(0, 200)}`);
             httpError.code = response.status;
             throw httpError;
        }

        let json;
        try {
             json = await response.json();
        } catch (parseError) {
            // console.warn(`[RLC] JSON parsing error for ${method} [Req ID ${requestId}]: ${parseError.message}`); // Reduce log noise
            parseError.code = 'JSON_PARSE_ERROR';
            throw parseError;
        }

        if (json.error) {
            const rpcError = new Error(`RPC Error: ${json.error.message} (Code: ${json.error.code})`);
            rpcError.code = json.error.code;
            throw rpcError;
        }

        if (!('result' in json)) {
             // console.warn(`[RLC] Received valid JSON RPC response but missing 'result' field for ${method} [Req ID ${requestId}]. Response:`, JSON.stringify(json).substring(0, 200)); // Reduce log noise
             const formatError = new Error("Invalid RPC response format: Missing 'result' field.");
             formatError.code = "INVALID_RESPONSE_FORMAT";
             throw formatError;
        }

        return json.result;
    }

    _calculateRetryDelay(retries) {
        const base = this.options.retryBaseDelay;
        const jitterFactor = this.options.retryJitter;
        const jitter = base * jitterFactor * (Math.random() - 0.5);
        const delay = Math.min(
             base * (2 ** retries) + jitter,
             this.options.retryMaxDelay
        );
        return Math.max(0, delay);
    }

    getRequestStats() {
        return {
             status: {
                 currentEndpoint: this.getCurrentEndpoint(),
                 currentEndpointIndex: this.currentEndpointIndex,
                 queueSize: this.requestQueue.length,
                 activeRequests: this.activeRequests,
                 consecutiveRateLimits: this.consecutiveRateLimits,
                 lastRateLimit: this.lastRateLimitTime > 0 ? new Date(this.lastRateLimitTime).toISOString() : 'None',
             },
             stats: {
                 ...this.stats,
                 successRate: (this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed) > 0 // Avoid division by zero
                      ? ((this.stats.totalRequestsSucceeded / (this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed)) * 100).toFixed(1) + '%'
                      : 'N/A'
             }
        };
    }
}

export default RateLimitedConnection;
