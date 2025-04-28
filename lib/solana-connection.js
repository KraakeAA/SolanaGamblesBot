// lib/solana-connection.js (Corrected Rotation via _rpcRequest Override)

import { Connection } from '@solana/web3.js';

// Helper to generate unique IDs for RPC requests
let nextRequestId = 1;
function getNextRequestId() {
    return nextRequestId++;
}

class RateLimitedConnection extends Connection {
    /**
     * Creates a rate-limited connection that rotates through multiple RPC endpoints.
     * @param {string[]} endpoints - An array of RPC endpoint URLs.
     * @param {object} options - Configuration options.
     * @param {string} [options.commitment='confirmed'] - Default commitment level.
     * @param {object} [options.httpHeaders={}] - Additional HTTP headers (applied to our fetch calls).
     * @param {string} [options.wsEndpoint] - Optional WebSocket endpoint (WS rotation needs careful handling).
     * @param {string} [options.clientId='multi-rpc-connection'] - Client identifier for headers.
     * @param {number} [options.maxConcurrent=5] - Max parallel requests.
     * @param {number} [options.retryBaseDelay=600] - Initial delay for retries (ms).
     * @param {number} [options.maxRetries=5] - Max attempts per request (per endpoint cycle).
     * @param {number} [options.rateLimitCooloff=5000] - Min pause duration after hitting 429 (ms).
     * @param {number} [options.retryMaxDelay=30000] - Max delay for exponential backoff (ms).
     * @param {number} [options.retryJitter=0.2] - Percentage jitter for retry delay (0.2 = +/- 20%).
     */
    constructor(endpoints, options = {}) {
        // --- 1. Validate Inputs ---
        if (!Array.isArray(endpoints) || endpoints.length === 0) {
            throw new Error("RateLimitedConnection requires a non-empty array of endpoint URLs.");
        }

        // --- 2. Prepare Arguments for super() ---
        // We still need to call super() with *an* endpoint, even though we override _rpcRequest.
        // Use the first endpoint initially.
        const initialEndpoint = endpoints[0];
        const clientId = options.clientId || `SolanaGamblesBot/2.1-multi-rpc (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`;

        const rpcOptions = {
            commitment: options.commitment || 'confirmed',
            // We don't strictly need httpHeaders here for super, as we apply them in our _rpcRequest override.
            // But keep it for potential base class usage or future compatibility.
             httpHeaders: {
                'Content-Type': 'application/json', // Keep content type
                'solana-client': clientId,
                ...(options.httpHeaders || {})
            },
            wsEndpoint: options.wsEndpoint || undefined,
            disableRetryOnRateLimit: true // Let OUR class handle retries/rate limits
        };

        // --- 3. Call super() FIRST ---
        console.log(`[RLC] Calling super() with initial endpoint: ${initialEndpoint}`);
        super(initialEndpoint, rpcOptions);
        console.log(`[RLC] super() call finished.`);

        // --- 4. Assign 'this' properties AFTER super() ---
        this.endpoints = [...endpoints];
        this.currentEndpointIndex = 0;

        // Store base headers for our fetch calls
        this.baseHttpHeaders = {
             'Content-Type': 'application/json',
             'solana-client': clientId,
             ...(options.httpHeaders || {})
        };

        // --- Configuration ---
        this.options = {
            maxConcurrent: options.maxConcurrent || 5,
            retryBaseDelay: options.retryBaseDelay || 600,
            maxRetries: options.maxRetries || 5,
            rateLimitCooloff: options.rateLimitCooloff || 5000,
            retryMaxDelay: options.retryMaxDelay || 30000,
            retryJitter: options.retryJitter || 0.2,
            wsEndpoint: options.wsEndpoint || undefined,
            // Store commitment for use in _rpcRequest if needed
            commitment: options.commitment || 'confirmed'
        };

        // --- State ---
        this.requestQueue = [];
        this.activeRequests = 0;
        this.lastRateLimitTime = 0;
        this.consecutiveRateLimits = 0;
        this.cooloffTimeout = null;

        // --- Statistics ---
        this.stats = {
            totalRequestsEnqueued: 0,
            totalRequestsFailed: 0,
            totalRequestsSucceeded: 0,
            rateLimitEvents: 0,
            endpointRotations: 0
        };

        // --- Initialization ---
        // No longer need _wrapRpcMethods, as we override _rpcRequest directly.
        console.log("[RLC] RateLimitedConnection (Multi-RPC - _rpcRequest Override) initialized with options:", this.options);
        console.log(`[RLC] Endpoints configured: ${this.endpoints.join(', ')}`);
    }

    // --- Multi-RPC Methods ---

    getCurrentEndpoint() {
        return this.endpoints[this.currentEndpointIndex];
    }

    getEndpointCount() {
        return this.endpoints.length;
    }

    // MODIFIED: Removed the problematic this.rpcEndpoint assignment
    _rotateEndpoint() {
        if (this.endpoints.length <= 1) {
            return this.getCurrentEndpoint();
        }

        const oldIndex = this.currentEndpointIndex;
        this.currentEndpointIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
        const newEndpoint = this.endpoints[this.currentEndpointIndex];

        // --- REMOVED ---
        // const oldRpcEndpoint = this.rpcEndpoint; // No longer needed directly
        // this.rpcEndpoint = newEndpoint; // <<< THIS WAS THE PROBLEM - REMOVED
        // --- --------- ---

        // Handle WebSocket rotation if necessary (remains complex)
        if (!this.options.wsEndpoint && this._wsEndpoint) {
             try {
                const url = new URL(newEndpoint);
                url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
                const newWsEndpoint = url.toString();
                if (this._wsEndpoint !== newWsEndpoint) {
                     console.log(`[RLC] Updating internal WebSocket endpoint reference to: ${newWsEndpoint} (Manual reconnect might be needed)`);
                     this._wsEndpoint = newWsEndpoint;
                     // TODO: Consider closing/reopening WS connection if active subscriptions exist
                }
            } catch(e) {
                console.error(`[RLC] Failed to derive WebSocket endpoint from ${newEndpoint}`, e);
            }
        }

        this.stats.endpointRotations++;
        console.log(`[RLC] Rotated RPC endpoint index from ${oldIndex} to ${this.currentEndpointIndex}. New active URL: ${newEndpoint}. Rotations: ${this.stats.endpointRotations}`);

        this.resetRateLimitCounters();
        return newEndpoint;
    }

    // --- Overridden Methods ---

    // Override _rpcRequest to handle endpoint selection and fetch directly
    /**
     * @param {string} method The RPC method name.
     * @param {unknown[]} args The RPC method arguments.
     * @returns {Promise<unknown>} A promise that resolves to the RPC result.
     * @override // Indicate that this overrides a base class method (optional annotation)
     * @private
     */
    async _rpcRequest(method, args) {
         // Enqueue the request to go through our rate limiting and retry logic
         return this._enqueueRequest(method, args);
    }

    // Renamed original _enqueueRequest to avoid conflict, now called by the overridden _rpcRequest
    async _enqueueRpcOperation(method, args) {
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

     // Alias _enqueueRequest to the new name for potential external calls if needed,
     // but internal calls now use _enqueueRpcOperation
     _enqueueRequest = this._enqueueRpcOperation;


    // _processQueue now calls _doRpcRequest directly
    async _processQueue() {
        // ... (Queue checking logic remains the same: concurrency, queue empty, cooloff) ...
         if (this.activeRequests >= this.options.maxConcurrent) return;
         if (this.requestQueue.length === 0) return;
         if (this.consecutiveRateLimits > 0) {
            const timeSinceLastRateLimit = Date.now() - this.lastRateLimitTime;
            const requiredCooloff = this.options.rateLimitCooloff * Math.pow(2, this.consecutiveRateLimits -1);
            const cappedCooloff = Math.min(requiredCooloff, this.options.retryMaxDelay);
            if (timeSinceLastRateLimit < cappedCooloff) {
                if (!this.cooloffTimeout) {
                    const remainingCooloff = cappedCooloff - timeSinceLastRateLimit;
                    this.cooloffTimeout = setTimeout(() => {
                        this.cooloffTimeout = null;
                        this._processQueue();
                    }, remainingCooloff + 50);
                }
                return;
            }
         }
        // ... (end queue checking)

        this.activeRequests++;
        const request = this.requestQueue.shift();

        try {
            // --- Execute the RPC call using our internal method ---
            const result = await this._doRpcRequest(request.method, request.args);

            // --- Success ---
            request.resolve(result);
            this.stats.totalRequestsSucceeded++;
            if (this.consecutiveRateLimits > 0) {
                this.consecutiveRateLimits = 0;
            }
        } catch (error) {
            // --- Error Handling & Retry Logic ---
            // Log includes endpoint used by _doRpcRequest implicitly
            console.warn(`[RLC] Error in ${request.method} (Attempt ${request.retries + 1}):`, error.message);

            const isRpcSpecific = this._isRpcSpecificError(error);
            const shouldRetry = this._shouldRetry(error, request.retries, isRpcSpecific);

            if (shouldRetry) {
                request.retries++;
                let rotated = false;
                if (isRpcSpecific && this.endpoints.length > 1) {
                    // Rotate endpoint logic (same as before)
                     const nextIndex = (this.currentEndpointIndex + 1) % this.endpoints.length;
                     // Only rotate if it's not back to the original immediately, unless only 2 endpoints
                     if (nextIndex !== request.originalEndpointIndex || this.endpoints.length <= 2 || request.retries >= this.endpoints.length) {
                         this._rotateEndpoint();
                         rotated = true;
                     } else {
                         // console.log(`[RLC] Avoiding immediate rotation back to original endpoint index ${request.originalEndpointIndex}.`); // Reduce noise
                     }
                }

                const delay = this._calculateRetryDelay(request.retries);
                console.log(`[RLC] Retrying ${request.method} after ${delay.toFixed(0)}ms delay (Retry ${request.retries}/${this.options.maxRetries}).${rotated ? ` New URL: ${this.getCurrentEndpoint()}.` : ''}`);

                try {
                    await new Promise(resolve => setTimeout(resolve, delay));
                    this._requeueRequest(request); // Requeue at the front
                } catch (requeueError) {
                    console.error(`[RLC] Error during requeue wait/logic for ${request.method}:`, requeueError);
                    this.stats.totalRequestsFailed++;
                    request.reject(error);
                }
            } else {
                // --- Do Not Retry: Final Failure ---
                console.error(`[RLC] Request ${request.method} failed permanently after ${request.retries} retries. Last endpoint used: ${this.getCurrentEndpoint()}. Error: ${error.message}`);
                this.stats.totalRequestsFailed++;
                request.reject(error);
            }
        } finally {
            // --- Cleanup ---
            this.activeRequests--;
            setImmediate(() => this._processQueue());
        }
    }

    /**
     * Performs the actual HTTP fetch for the RPC request.
     * @param {string} method The RPC method name.
     * @param {unknown[]} params The RPC method arguments.
     * @returns {Promise<unknown>} A promise that resolves to the RPC result.
     * @private
     */
    async _doRpcRequest(method, params) {
        const currentEndpoint = this.getCurrentEndpoint();
        const requestId = getNextRequestId(); // Get unique ID for this request
        const payload = {
            jsonrpc: '2.0',
            id: requestId,
            method: method,
            params: params,
        };

        // Add minor jitter before the request
        const jitter = Math.random() * 50; // 0-50ms jitter
        await new Promise(r => setTimeout(r, jitter));

        // console.log(`[RLC] Sending RPC ${method} (ID: ${requestId}) to ${currentEndpoint}`); // Verbose

        let response;
        try {
            response = await fetch(currentEndpoint, {
                method: 'POST',
                headers: this.baseHttpHeaders, // Use stored headers
                body: JSON.stringify(payload),
                signal: AbortSignal.timeout(30000) // 30 second timeout for the fetch itself
            });
        } catch (fetchError) {
             // Network errors during fetch
             console.error(`[RLC] Fetch error for ${method} on ${currentEndpoint}:`, fetchError.message);
             // Rethrow with a potential status code if available (e.g., from AbortSignal timeout)
             fetchError.code = fetchError.name === 'TimeoutError' ? 504 : 503; // Map Timeout to Gateway Timeout, others to Service Unavailable
             throw fetchError;
        }


        // Handle non-200 HTTP status codes
        if (!response.ok) {
             const status = response.status;
             let errorText = response.statusText;
             try {
                // Try to get more detail from the response body
                 errorText = await response.text();
             } catch (_) { /* ignore */ }

             console.error(`[RLC] HTTP Error ${status} for ${method} on ${currentEndpoint}: ${errorText.slice(0, 500)}`); // Log first 500 chars
              // Create an error object that mimics RPC errors for retry logic
              const httpError = new Error(`HTTP error ${status}: ${errorText.slice(0,100)}`);
              httpError.code = status; // Attach status code for retry logic
              httpError.response = { status }; // Add for compatibility with _shouldRetry
              throw httpError;
        }

        // Parse JSON response
        let responseJson;
        try {
            responseJson = await response.json();
        } catch (parseError) {
            console.error(`[RLC] JSON parse error for ${method} response from ${currentEndpoint}:`, parseError.message);
            // Treat as a server error
             const jsonError = new Error(`Failed to parse JSON response: ${parseError.message}`);
             jsonError.code = 500; // Internal Server Error (or similar)
             throw jsonError;
        }

        // Check for JSON-RPC level errors
        if (responseJson.error) {
            console.warn(`[RLC] RPC Error for ${method} (ID: ${requestId}) on ${currentEndpoint}:`, responseJson.error);
             // Throw an error object that includes the RPC error details
             // Use RpcError if available, otherwise a generic Error
              const rpcErrorData = responseJson.error;
              let errorToThrow;
              if (typeof RpcError !== 'undefined') {
                   errorToThrow = new RpcError(rpcErrorData);
              } else {
                   errorToThrow = new Error(`RPC error ${rpcErrorData.code}: ${rpcErrorData.message}`);
                   errorToThrow.code = rpcErrorData.code; // Attach code if possible
                   errorToThrow.data = rpcErrorData.data;
              }
               // Ensure the error object has properties our retry logic might check
               errorToThrow.message = `RPC error ${rpcErrorData.code}: ${rpcErrorData.message}`; // Ensure message is set
               if (!errorToThrow.code) errorToThrow.code = rpcErrorData.code;

               throw errorToThrow;
        }

        // Check if the response ID matches the request ID (important!)
        if (responseJson.id !== requestId) {
             console.warn(`[RLC] RPC Response ID mismatch! Expected ${requestId}, got ${responseJson.id}. Endpoint: ${currentEndpoint}`);
             // Treat as an error, maybe retry? For now, throw.
             const idMismatchError = new Error(`RPC Response ID mismatch. Expected ${requestId}, got ${responseJson.id}`);
             idMismatchError.code = 500; // Treat as server error
             throw idMismatchError;
        }


        // Return the successful result
        return responseJson.result;
    }

    // ... ( _calculateRetryDelay, _shouldRetry, _isRpcSpecificError, _requeueRequest remain the same ) ...
     _calculateRetryDelay(retryCount) {
        const baseDelay = this.options.retryBaseDelay;
        const maxDelay = this.options.retryMaxDelay;
        const jitterRatio = this.options.retryJitter;
        const backoffFactor = Math.pow(2, this.consecutiveRateLimits);
        const exponentialDelay = baseDelay * Math.pow(2, retryCount -1) * backoffFactor;
        const cappedDelay = Math.min(exponentialDelay, maxDelay);
        const jitterMagnitude = cappedDelay * jitterRatio;
        const jitter = (Math.random() * 2 - 1) * jitterMagnitude;
        const finalDelay = Math.max(50, cappedDelay + jitter);
         if (this.consecutiveRateLimits > 0) {
              const requiredCooloff = this.options.rateLimitCooloff * Math.pow(2, this.consecutiveRateLimits -1);
              const cappedCooloff = Math.min(requiredCooloff, maxDelay);
             return Math.max(finalDelay, cappedCooloff);
         }
        return finalDelay;
    }
     _shouldRetry(error, retryCount, isRpcSpecific) {
         if (retryCount >= this.options.maxRetries) {
             return false;
         }
         const errorMessage = error?.message?.toLowerCase() || '';
         const errorStatus = error?.response?.status || error?.statusCode || error?.code;
         if (errorStatus === 429 || String(errorStatus) === '429' || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
              if (this.lastRateLimitTime === 0 || (Date.now() - this.lastRateLimitTime) >= this.options.rateLimitCooloff) {
                 this.consecutiveRateLimits++;
                 this.stats.rateLimitEvents++;
                 console.warn(`[RLC] Rate limit detected! Consecutive: ${this.consecutiveRateLimits}. Cooling off.`);
              }
              this.lastRateLimitTime = Date.now();
             return true;
         }
         const retryableStatuses = [500, 502, 503, 504];
         if (retryableStatuses.includes(Number(errorStatus))) {
            return true;
         }
          const retryableMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'connection refused'];
          const retryableCodes = ['etimedout', 'econnreset', 'enetunreach', 'eai_again', 'econnaborted', 'econnrefused'];
          if (retryableMessages.some(msg => errorMessage.includes(msg)) || (error?.code && retryableCodes.includes(error.code.toLowerCase()))) {
              return true;
          }
           if (isRpcSpecific && (errorMessage.includes('transaction simulation failed') || errorMessage.includes('failed to simulate'))) {
               // console.log(`[RLC] Retrying simulation failure (potential temporary RPC load).`); // Reduce noise
               return true;
           }
         return false;
     }
      _isRpcSpecificError(error) {
          const errorMessage = error?.message?.toLowerCase() || '';
          const errorStatus = error?.response?.status || error?.statusCode || error?.code;
          if (errorStatus === 429 || String(errorStatus) === '429' || errorMessage.includes('429') || errorMessage.includes('rate limit') || errorMessage.includes('too many requests')) {
              return true;
          }
          if ([500, 502, 503, 504].includes(Number(errorStatus))) {
               return true;
          }
           const networkErrorMessages = ['timeout', 'timed out', 'econnreset', 'esockettimedout', 'econnrefused', 'failed to fetch', 'socket hang up', 'connection refused'];
           const networkErrorCodes = ['etimedout', 'econnreset', 'econnaborted', 'econnrefused'];
           if (networkErrorMessages.some(msg => errorMessage.includes(msg)) || (error?.code && networkErrorCodes.includes(error.code.toLowerCase())) ) {
               return true;
           }
          if (errorMessage.includes('transaction simulation failed') || errorMessage.includes('failed to simulate')) {
               return true;
          }
           if (errorMessage.includes('health check failed')) {
               return true;
           }
          return false;
        }
     _requeueRequest(request) {
        this.requestQueue.unshift(request);
    }


    // ... ( getRequestStats, resetRateLimitCounters remain the same ) ...
     getRequestStats() {
        const completedRequests = this.stats.totalRequestsSucceeded + this.stats.totalRequestsFailed;
        const successRate = completedRequests > 0
            ? ((this.stats.totalRequestsSucceeded) / completedRequests * 100).toFixed(2)
            : 'N/A';
        return {
            options: { maxConcurrent: this.options.maxConcurrent, maxRetries: this.options.maxRetries, rateLimitCooloff: this.options.rateLimitCooloff, retryBaseDelay: this.options.retryBaseDelay, endpointsCount: this.endpoints.length },
            status: { queueSize: this.requestQueue.length, activeRequests: this.activeRequests, currentEndpoint: this.getCurrentEndpoint(), currentEndpointIndex: this.currentEndpointIndex, consecutiveRateLimits: this.consecutiveRateLimits, lastRateLimit: this.lastRateLimitTime > 0 ? `${Math.floor((Date.now() - this.lastRateLimitTime) / 1000)}s ago` : 'never', isCoolingOff: !!this.cooloffTimeout },
            stats: { totalEnqueued: this.stats.totalRequestsEnqueued, totalSucceeded: this.stats.totalRequestsSucceeded, totalFailed: this.stats.totalRequestsFailed, rateLimitEvents: this.stats.rateLimitEvents, endpointRotations: this.stats.endpointRotations, successRate: `${successRate}%` }
        };
    }
     resetRateLimitCounters() {
        this.consecutiveRateLimits = 0;
        if (this.cooloffTimeout) {
            clearTimeout(this.cooloffTimeout);
            this.cooloffTimeout = null;
        }
    }

} // End class RateLimitedConnection

export default RateLimitedConnection;
