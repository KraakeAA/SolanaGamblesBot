// lib/solana-connection.js (EDITED for explicit wsEndpoint handling)

import { Connection } from '@solana/web3.js';

let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

// --- Define your specific endpoint details ---
// --- Replace with your actual API keys/paths if they change ---
const HELIUS_HTTPS_URL = 'https://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';
const HELIUS_WSS_URL = 'wss://mainnet.helius-rpc.com/?api-key=62432b60-98a8-4f7f-9cc2-43d583f8d025';

const QUICKNODE_HTTPS_URL = 'https://multi-young-tab.solana-mainnet.quiknode.pro/56662595a48eb3798b005654091f77aa5673e15e/';
// Derive QuickNode WSS by replacing https with wss (Check QuickNode dashboard if issues)
const QUICKNODE_WSS_URL = QUICKNODE_HTTPS_URL.replace(/^https:/, 'wss:');
// --- End endpoint definitions ---


class RateLimitedConnection extends Connection {
    constructor(endpoints, options = {}) {
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        // Ensure URLs only contain the expected Helius and QuickNode endpoints
        const validEndpoints = endpoints.filter(ep => ep === HELIUS_HTTPS_URL || ep === QUICKNODE_HTTPS_URL);
        if (validEndpoints.length !== endpoints.length) {
             console.warn("[RLC] Constructor Warning: Input endpoints array contains URLs other than the expected Helius and QuickNode URLs. Only using valid ones.", { input: endpoints, valid: validEndpoints });
             if (validEndpoints.length === 0) {
                 throw new Error("RateLimitedConnection: No valid Helius or QuickNode endpoints provided in the endpoints array.");
             }
             endpoints = validEndpoints; // Use only the valid endpoints
        }


        const initialEndpoint = endpoints[0];
        const clientId = options.clientId || `SolanaGamblesBot/2.2-multi-rpc (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`; // Updated version

        // --- Determine initial WSS endpoint ---
        const initialWssEndpoint = this._getWssEndpoint(initialEndpoint);
        if (!initialWssEndpoint) {
            console.warn(`[RLC] Could not determine initial WSS endpoint for ${initialEndpoint}. WebSocket features might fail initially.`);
        }
        console.log(`[RLC] Determined initial WSS endpoint: ${initialWssEndpoint || 'None'}`);
        // --- End WSS determination ---

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            httpHeaders: { // Keep httpHeaders separate
                'Content-Type': 'application/json',
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: initialWssEndpoint, // Pass explicit WSS endpoint for initial connection
            disableRetryOnRateLimit: true, // Let our custom logic handle retries/rotation
            fetch: fetch // Explicitly provide fetch if needed in specific environments
        };

        console.log(`[RLC] Connecting to initial RPC endpoint: ${initialEndpoint}`);
        super(initialEndpoint, rpcOptions); // Call parent constructor

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
            retryBaseDelay: options.retryBaseDelay || 600, // Base delay for retries
            maxRetries: options.maxRetries || 5,         // Max retries per request *before giving up or rotating hard*
            rateLimitCooloff: options.rateLimitCooloff || 5000, // Base cooloff after 429
            retryMaxDelay: options.retryMaxDelay || 30000,     // Max delay cap for retries/cooloff
            retryJitter: options.retryJitter || 0.2,          // Jitter for retry delay
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
    }

    // --- Helper to get WSS endpoint ---
    _getWssEndpoint(httpsUrl) {
        if (httpsUrl === HELIUS_HTTPS_URL) {
            return HELIUS_WSS_URL;
        } else if (httpsUrl === QUICKNODE_HTTPS_URL) {
            return QUICKNODE_WSS_URL;
        } else {
            console.warn(`[RLC Helper] Unknown HTTPS URL provided: ${httpsUrl}. Cannot determine specific WSS URL.`);
            // Fallback: try simple replacement, might work for some providers
            try {
                return httpsUrl.replace(/^https:/, 'wss:');
            } catch (e) {
                return undefined; // Return undefined if replacement fails
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
        const newWssEndpoint = this._getWssEndpoint(newEndpoint); // Get corresponding WSS endpoint

        this.stats.endpointRotations++;
        console.log(`[RLC] Rotated RPC from index ${oldIndex} to ${this.currentEndpointIndex}: ${newEndpoint}`);

        // --- CRITICAL: Update parent Connection state ---
        this._rpcEndpoint = newEndpoint;
        this._rpcWsEndpoint = newWssEndpoint; // Set the determined WSS endpoint
        console.log(`[RLC] Updated internal Connection state: HTTP=${this._rpcEndpoint}, WSS=${this._rpcWsEndpoint || 'Not Set'}`);
        // --- End Update ---

        this.consecutiveRateLimits = 0; // Reset rate limit count on successful rotation
        return newEndpoint;
    }

    // Override _rpcRequest to use our queuing and custom fetch logic
    async _rpcRequest(method, args) {
        // Note: This bypasses the parent Connection's direct _rpcRequest
        // We handle retries, rotation, rate limits via the queue and _doRpcRequest
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
                originalEndpointIndex: this.currentEndpointIndex // Track where it started
            };
            this.requestQueue.push(request);
            // Use setImmediate to defer queue processing slightly, preventing potential stack overflows
            setImmediate(() => this._processQueue());
        });
    }

    async _processQueue() {
        // Prevent concurrent processing beyond the limit
        if (this.activeRequests >= this.options.maxConcurrent) return;
        if (this.requestQueue.length === 0) return;

        // --- Rate Limit Cooloff Check ---
        if (this.consecutiveRateLimits > 0) {
            const timeSinceLast = Date.now() - this.lastRateLimitTime;
            // Exponential backoff for cooloff, capped by retryMaxDelay
            const cooloff = Math.min(
                 this.options.rateLimitCooloff * (2 ** (this.consecutiveRateLimits - 1)),
                 this.options.retryMaxDelay
            );

            if (timeSinceLast < cooloff) {
                // If already cooling off, don't set another timeout
                if (!this.cooloffTimeout) {
                    const delay = cooloff - timeSinceLast + 50; // Add small buffer
                    console.log(`[RLC] Rate limit cooloff active. Waiting ${delay.toFixed(0)}ms.`);
                    this.cooloffTimeout = setTimeout(() => {
                        this.cooloffTimeout = null; // Clear timeout ID
                        this._processQueue(); // Re-attempt processing
                    }, delay);
                    if (this.cooloffTimeout.unref) this.cooloffTimeout.unref(); // Allow process to exit if only this timeout is active
                }
                return; // Don't process queue while cooling off
            } else {
                 // Cooloff period has passed
                 console.log(`[RLC] Rate limit cooloff period ended (${cooloff.toFixed(0)}ms). Resuming processing.`);
                 // Don't reset consecutiveRateLimits here, reset only on success
            }
        }
        // --- End Cooloff Check ---


        this.activeRequests++;
        const request = this.requestQueue.shift();
        const requestId = getNextRequestId(); // Get ID for logging this attempt

        try {
            // Use the currently selected endpoint for this attempt
            const currentEndpoint = this.getCurrentEndpoint();
            const result = await this._doRpcRequest(currentEndpoint, request.method, request.args, requestId);

            // console.log(`[RLC] RPC Success: ${request.method} [Req ID ${requestId}]`); // Verbose Success Log
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            this.consecutiveRateLimits = 0; // Reset rate limit counter on success

        } catch (error) {
            console.warn(`[RLC] RPC Attempt Failed: ${request.method} [Req ID ${requestId}] using ${this.getCurrentEndpoint()}. Error: ${error.message} (Code: ${error.code || 'N/A'})`);

            // --- Handle Rate Limit (429) ---
            if (error.code === 429 || error.message?.includes('429')) {
                this.stats.rateLimitEvents++;
                this.lastRateLimitTime = Date.now();
                this.consecutiveRateLimits++;
                console.warn(`[RLC] Rate limit detected (Consecutive: ${this.consecutiveRateLimits}). Rotating endpoint and starting cooloff.`);

                // Rotate endpoint immediately on rate limit
                this._rotateEndpoint();

                // Re-queue the request WITHOUT incrementing retries (cooloff handles delay)
                this.requestQueue.unshift(request);
                // Trigger cooloff delay check in the next _processQueue cycle
                setImmediate(() => this._processQueue());

            } else { // --- Handle Other Errors ---
                 const shouldRetry = request.retries < this.options.maxRetries;
                 if (shouldRetry) {
                     request.retries++;
                     console.log(`[RLC] Retrying request ${requestId} (${request.method}). Attempt ${request.retries}/${this.options.maxRetries}. Rotating endpoint.`);

                     // Rotate endpoint on other retryable errors too
                     this._rotateEndpoint();

                     const delay = this._calculateRetryDelay(request.retries);
                     console.log(`[RLC] Scheduling retry ${requestId} in ${delay.toFixed(0)}ms.`);
                     setTimeout(() => {
                         this.requestQueue.unshift(request); // Add back to the front for retry
                         this._processQueue();
                     }, delay);
                 } else {
                     // Max retries reached for a non-rate-limit error
                     console.error(`[RLC] RPC Request Failed Permanently: ${request.method} [Req ID ${requestId}] after ${request.retries} retries. Final Error: ${error.message}`);
                     this.stats.totalRequestsFailed++;
                     request.reject(error); // Reject the promise
                     // Reset rate limits only if the failure wasn't a rate limit itself
                     if (this.consecutiveRateLimits > 0) {
                         console.log("[RLC] Resetting consecutive rate limit count due to different final error.");
                         this.consecutiveRateLimits = 0;
                     }
                 }
            }
        } finally {
            // Ensure activeRequests is decremented even if errors occur
            this.activeRequests--;
            // Trigger processing for the next item if possible
            setImmediate(() => this._processQueue());
        }
    }

    // Updated _doRpcRequest to accept endpoint explicitly for clarity
    async _doRpcRequest(endpoint, method, params, requestId) {
        const payload = {
            jsonrpc: '2.0',
            id: requestId, // Use request-specific ID
            method,
            params
        };

        // Small random delay before sending to potentially avoid thundering herd
        // const jitter = Math.random() * 20; // Reduced jitter
        // await new Promise(r => setTimeout(r, jitter));

        let response;
        try {
            response = await fetch(endpoint, {
                method: 'POST',
                headers: this.baseHttpHeaders,
                body: JSON.stringify(payload),
                 // Add a timeout for the fetch request itself (e.g., 30 seconds)
                signal: AbortSignal.timeout(30000) // Adjust timeout as needed
            });
        } catch (fetchError) {
            // Catch fetch-specific errors (e.g., network timeout, DNS resolution)
            console.warn(`[RLC] Fetch error for ${method} [Req ID ${requestId}] to ${endpoint}: ${fetchError.name} - ${fetchError.message}`);
             // Re-throw fetch errors so the retry logic catches them
             // Add a code if possible (e.g., from AbortSignal timeout)
             if (fetchError.name === 'TimeoutError' || fetchError.name === 'AbortError') {
                 fetchError.code = 'FETCH_TIMEOUT';
             } else {
                  fetchError.code = 'FETCH_ERROR'; // Generic fetch error
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
             const httpError = new Error(`HTTP ${response.status} ${response.statusText}: ${errorText.substring(0, 200)}`); // Limit error text length
             httpError.code = response.status; // Set HTTP status code
             // console.warn(`[RLC] HTTP error response details for ${method} [Req ID ${requestId}]: ${errorText}`); // Log full error if needed
             throw httpError; // Throw so retry logic catches it
        }

        let json;
        try {
             json = await response.json();
        } catch (parseError) {
            console.warn(`[RLC] JSON parsing error for ${method} [Req ID ${requestId}]: ${parseError.message}`);
            parseError.code = 'JSON_PARSE_ERROR';
            throw parseError; // Throw so retry logic catches it
        }


        if (json.error) {
            const rpcError = new Error(`RPC Error: ${json.error.message} (Code: ${json.error.code})`);
            rpcError.code = json.error.code; // Set RPC error code
            // console.warn(`[RLC] RPC error details for ${method} [Req ID ${requestId}]:`, json.error); // Log full error object if needed
            throw rpcError; // Throw so retry logic catches it
        }

        if (!('result' in json)) {
             console.warn(`[RLC] Received valid JSON RPC response but missing 'result' field for ${method} [Req ID ${requestId}]. Response:`, JSON.stringify(json).substring(0, 200));
             const formatError = new Error("Invalid RPC response format: Missing 'result' field.");
             formatError.code = "INVALID_RESPONSE_FORMAT";
             throw formatError;
        }

        return json.result;
    }

    _calculateRetryDelay(retries) {
        const base = this.options.retryBaseDelay;
        const jitterFactor = this.options.retryJitter; // e.g., 0.2
        // Calculate jitter: +/- (base * jitterFactor / 2)
        const jitter = base * jitterFactor * (Math.random() - 0.5);
        // Exponential backoff + jitter, capped by maxDelay
        const delay = Math.min(
             base * (2 ** retries) + jitter,
             this.options.retryMaxDelay
        );
        // Ensure delay is not negative
        return Math.max(0, delay);
    }

    // --- Add method to get current stats ---
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
                 successRate: this.stats.totalRequestsEnqueued > 0
                      ? ((this.stats.totalRequestsSucceeded / (this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed)) * 100).toFixed(1) + '%'
                      : 'N/A'
             }
        };
    }
    // --- End stats method ---
}

export default RateLimitedConnection;
