// ==UserScript==
// @name         LMArena API Bridge
// @namespace    http://tampermonkey.net/
// @version      2.5
// @description  Bridges LMArena to a local API server via WebSocket for streamlined automation.
// @author       Lianues
// @match        https://lmarena.ai/*
// @match        https://*.lmarena.ai/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=lmarena.ai
// @grant        none
// @run-at       document-end
// ==/UserScript==

(function () {
    'use strict';

    // --- Configuration ---
    const SERVER_URL = "ws://localhost:5102/ws"; // Must align with api_server.py
    let socket;
    let isCaptureModeActive = false; // Toggle for ID capture mode

    // --- Core logic ---
    function connect() {
        console.log(`[API Bridge] Connecting to local server: ${SERVER_URL}...`);
        socket = new WebSocket(SERVER_URL);

        socket.onopen = () => {
            console.log('[API Bridge] ✅ WebSocket connection to the local server established.');
            document.title = '✅ ' + document.title;
        };

        socket.onmessage = async (event) => {
            try {
                const message = JSON.parse(event.data);

                // Handle control commands separately from chat payloads
                if (message.command) {
                    console.log(`[API Bridge] ⬇️ Command received: ${message.command}`);
                    if (message.command === 'refresh' || message.command === 'reconnect') {
                        console.log(`[API Bridge] Executing page reload due to '${message.command}' command...`);
                        location.reload();
                    } else if (message.command === 'activate_id_capture') {
                        console.log('[API Bridge] ✅ ID capture mode activated. Click "Retry" on the page to capture IDs.');
                        isCaptureModeActive = true;
                        document.title = '🎯 ' + document.title;
                    } else if (message.command === 'send_page_source') {
                       console.log('[API Bridge] Command received: send page source. Uploading...');
                       sendPageSource();
                    }
                    return;
                }

                const { request_id, payload } = message;

                if (!request_id || !payload) {
                    console.error('[API Bridge] Invalid message from server:', message);
                    return;
                }

                console.log(`[API Bridge] ⬇️ Chat request ${request_id.substring(0, 8)} received. Executing fetch...`);
                await executeFetchAndStreamBack(request_id, payload);

            } catch (error) {
                console.error('[API Bridge] Error handling server message:', error);
            }
        };

        socket.onclose = () => {
            console.warn('[API Bridge] 🔌 Connection closed. Retrying in 5 seconds...');
            if (document.title.startsWith('✅ ')) {
                document.title = document.title.substring(2);
            }
            setTimeout(connect, 5000);
        };

        socket.onerror = (error) => {
            console.error('[API Bridge] ❌ WebSocket error:', error);
            socket.close(); // Trigger reconnection logic in onclose
        };
    }

    async function executeFetchAndStreamBack(requestId, payload) {
        console.log(`[API Bridge] Current hostname: ${window.location.hostname}`);
        const { is_image_request, message_templates, target_model_id, session_id, message_id } = payload;

        // --- Validate session identifiers ---
        if (!session_id || !message_id) {
            const errorMsg = 'Session information (session_id or message_id) is missing. Run `id_updater.py` to configure them.';
            console.error(`[API Bridge] ${errorMsg}`);
            sendToServer(requestId, { error: errorMsg });
            sendToServer(requestId, '[DONE]');
            return;
        }

        const apiUrl = `/nextjs-api/stream/retry-evaluation-session-message/${session_id}/messages/${message_id}`;
        const httpMethod = 'PUT';

        console.log(`[API Bridge] Using API endpoint: ${apiUrl}`);

        const newMessages = [];
        let lastMsgIdInChain = null;

        if (!message_templates || message_templates.length === 0) {
            const errorMsg = 'Message template list from backend is empty.';
            console.error(`[API Bridge] ${errorMsg}`);
            sendToServer(requestId, { error: errorMsg });
            sendToServer(requestId, '[DONE]');
            return;
        }

        // Prepare the message chain for both text and image workflows
        for (let i = 0; i < message_templates.length; i++) {
            const template = message_templates[i];
            const currentMsgId = crypto.randomUUID();
            const parentIds = lastMsgIdInChain ? [lastMsgIdInChain] : [];

            const status = is_image_request ? 'success' : ((i === message_templates.length - 1) ? 'pending' : 'success');

            newMessages.push({
                role: template.role,
                content: template.content,
                id: currentMsgId,
                evaluationId: null,
                evaluationSessionId: session_id,
                parentMessageIds: parentIds,
                experimental_attachments: Array.isArray(template.attachments) ? template.attachments : [],
                failureReason: null,
                metadata: null,
                participantPosition: template.participantPosition || 'a',
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                status: status,
            });
            lastMsgIdInChain = currentMsgId;
        }

        const body = {
            messages: newMessages,
            modelId: target_model_id,
        };

        console.log('[API Bridge] Final payload for LMArena API:', JSON.stringify(body, null, 2));

        window.isApiBridgeRequest = true;
        try {
            const response = await fetch(apiUrl, {
                method: httpMethod,
                headers: {
                    'Content-Type': 'text/plain;charset=UTF-8', // LMArena expects text/plain
                    'Accept': '*/*',
                },
                body: JSON.stringify(body),
                credentials: 'include' // Cookies are required
            });

            if (!response.ok || !response.body) {
                const errorBody = await response.text();
                throw new Error(`Unexpected response. Status: ${response.status}. Body: ${errorBody}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            while (true) {
                const { value, done } = await reader.read();
                if (done) {
                    console.log(`[API Bridge] ✅ Stream for request ${requestId.substring(0, 8)} completed.`);
                    sendToServer(requestId, '[DONE]');
                    break;
                }
                const chunk = decoder.decode(value);
                sendToServer(requestId, chunk);
            }

        } catch (error) {
            console.error(`[API Bridge] ❌ Fetch failed for request ${requestId.substring(0, 8)}:`, error);
            sendToServer(requestId, { error: error.message });
        } finally {
            window.isApiBridgeRequest = false;
        }
    }

    function sendToServer(requestId, data) {
        if (socket && socket.readyState === WebSocket.OPEN) {
            const message = {
                request_id: requestId,
                data: data
            };
            socket.send(JSON.stringify(message));
        } else {
            console.error('[API Bridge] Cannot send data because the WebSocket is not open.');
        }
    }

    // --- Fetch interception ---
    const originalFetch = window.fetch;
    window.fetch = function(...args) {
        const urlArg = args[0];
        let urlString = '';

        if (urlArg instanceof Request) {
            urlString = urlArg.url;
        } else if (urlArg instanceof URL) {
            urlString = urlArg.href;
        } else if (typeof urlArg === 'string') {
            urlString = urlArg;
        }

        if (urlString) {
            const match = urlString.match(/\/nextjs-api\/stream\/retry-evaluation-session-message\/([a-f0-9-]+)\/messages\/([a-f0-9-]+)/);

            // Only capture IDs when the request is not triggered by the bridge itself and capture mode is active
            if (match && !window.isApiBridgeRequest && isCaptureModeActive) {
                const sessionId = match[1];
                const messageId = match[2];
                console.log('[API Bridge Interceptor] 🎯 Captured session/message IDs while in capture mode.');

                isCaptureModeActive = false;
                if (document.title.startsWith('🎯 ')) {
                    document.title = document.title.substring(2);
                }

                fetch('http://127.0.0.1:5103/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sessionId, messageId })
                })
                .then(response => {
                    if (!response.ok) throw new Error(`Server responded with status: ${response.status}`);
                    console.log('[API Bridge] ✅ ID update sent successfully. Capture mode disabled.');
                })
                .catch(err => {
                    console.error('[API Bridge] Error sending captured IDs:', err.message);
                });
            }
        }

        return originalFetch.apply(this, args);
    };

    // --- Page source upload ---
    async function sendPageSource() {
        try {
            const htmlContent = document.documentElement.outerHTML;
            await fetch('http://localhost:5102/internal/update_available_models', {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html; charset=utf-8'
                },
                body: htmlContent
            });
            console.log('[API Bridge] Page source sent successfully.');
        } catch (e) {
            console.error('[API Bridge] Failed to send page source:', e);
        }
    }

    // --- Startup ---
    console.log('========================================');
    console.log('  LMArena API Bridge v2.5 running.');
    console.log('  - Chat bridge: ws://localhost:5102');
    console.log('  - ID updater target: http://localhost:5103');
    console.log('========================================');

    connect();

})();
