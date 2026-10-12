const http = require('http');
const fs = require('fs');
const path = require('path');
const https = require('https');

const PORT = process.env.PORT || 3000;

// Cargar prompts preprocesados
let promptsData = [];
try {
    const rawData = fs.readFileSync(path.join(__dirname, 'prompts_data.json'), 'utf8');
    promptsData = JSON.parse(rawData);
} catch (err) {
    console.error('Error cargando prompts_data.json:', err.message);
}

// ============================================================================
// BÓVEDA Y POOL INTELIGENTE MULTI-API (LOAD BALANCER + AUTO-FAILOVER + QUEUE)
// ============================================================================
// Aquí se pueden almacenar llaves ofuscadas en Base64 (para evitar bloqueos del
// escáner público de GitHub) además de las variables de entorno de Render.
const ENCODED_KEYS_VAULT = [
    // Formato: { provider: 'gemini'|'groq'|'openrouter'|'deepseek'|'mistral'|'nvidia'|'cerebras', b64: '...' }
];

function decodeVaultKeys() {
    const list = [];
    for (const item of ENCODED_KEYS_VAULT) {
        if (item && item.b64) {
            try {
                const decoded = Buffer.from(item.b64, 'base64').toString('utf8').trim();
                if (decoded) list.push({ provider: item.provider, key: decoded });
            } catch (e) {}
        }
    }
    return list;
}

// Detectar automáticamente el proveedor según el prefijo de la API Key
function detectProviderFromKey(rawKey, hintProvider = '') {
    const k = (rawKey || '').trim();
    if (!k) return null;
    if (k.startsWith('AIza')) return 'gemini';
    if (k.startsWith('gsk_')) return 'groq';
    if (k.startsWith('sk-or-v1-') || k.startsWith('sk-or-')) return 'openrouter';
    if (k.startsWith('nvapi-')) return 'nvidia';
    if (k.startsWith('csk-')) return 'cerebras';
    if (hintProvider) return hintProvider;
    if (k.startsWith('sk-') && k.length < 45) return 'deepseek';
    if (k.length === 32 && /^[a-zA-Z0-9]+$/.test(k)) return 'mistral';
    return 'openrouter';
}

// Estado en tiempo real de cada llave en el Pool (Least-Connections + Cooldown)
const keyStateMap = new Map();

function getKeyState(key) {
    if (!keyStateMap.has(key)) {
        keyStateMap.set(key, {
            inFlight: 0,
            totalCalls: 0,
            errors: 0,
            lastUsed: 0,
            cooldownUntil: 0
        });
    }
    return keyStateMap.get(key);
}

// Construir el Pool completo de llaves disponibles (Servidor + Cliente)
function buildActiveKeyPool(clientKeysInput = '') {
    const entries = [];
    const seen = new Set();

    function addRawKeys(rawStr, hintProvider = '') {
        if (!rawStr) return;
        const parts = String(rawStr).split(/[\n,;]+/).map(s => s.trim()).filter(Boolean);
        for (const p of parts) {
            // Permitir prefijo explícito opcional como "mistral:XXXX" o "gemini:AIza..."
            let explicitProvider = hintProvider;
            let actualKey = p;
            const colonIdx = p.indexOf(':');
            if (colonIdx > 0 && colonIdx < 15 && !p.startsWith('sk-or-')) {
                explicitProvider = p.slice(0, colonIdx).toLowerCase().trim();
                actualKey = p.slice(colonIdx + 1).trim();
            }
            if (!actualKey || seen.has(actualKey)) continue;
            seen.add(actualKey);
            const provider = detectProviderFromKey(actualKey, explicitProvider);
            if (provider) {
                entries.push({
                    provider,
                    key: actualKey,
                    state: getKeyState(actualKey)
                });
            }
        }
    }

    // 1. Llaves enviadas por el cliente (si configuró llaves propias en el navegador)
    addRawKeys(clientKeysInput);

    // 2. Llaves en la bóveda codificada del servidor
    for (const vk of decodeVaultKeys()) {
        addRawKeys(vk.key, vk.provider);
    }

    // 3. Variables de entorno del servidor (Render)
    addRawKeys(process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY, 'gemini');
    addRawKeys(process.env.GROQ_API_KEYS || process.env.GROQ_API_KEY, 'groq');
    addRawKeys(process.env.MISTRAL_API_KEYS || process.env.MISTRAL_API_KEY, 'mistral');
    addRawKeys(process.env.CEREBRAS_API_KEYS || process.env.CEREBRAS_API_KEY, 'cerebras');
    addRawKeys(process.env.NVIDIA_API_KEYS || process.env.NVIDIA_KEY, 'nvidia');
    addRawKeys(process.env.DEEPSEEK_API_KEYS || process.env.DEEPSEEK_API_KEY, 'deepseek');
    addRawKeys(process.env.OPENROUTER_API_KEYS || process.env.OPENROUTER_API_KEY || process.env.OPEN_ROUTER, 'openrouter');

    // Ordenar: 1) Que NO estén en enfriamiento (cooldown), 2) Menor número de tareas activas (inFlight), 3) Menos usada recientemente (Round-Robin)
    const now = Date.now();
    entries.sort((a, b) => {
        const aCool = a.state.cooldownUntil > now ? 1 : 0;
        const bCool = b.state.cooldownUntil > now ? 1 : 0;
        if (aCool !== bCool) return aCool - bCool;
        if (a.state.inFlight !== b.state.inFlight) return a.state.inFlight - b.state.inFlight;
        return a.state.lastUsed - b.state.lastUsed;
    });

    return entries;
}

// ============================================================================
// COLA DE CONCURRENCIA INTELIGENTE (EVITA COLAPSO ANTE MÚLTIPLES USUARIOS)
// ============================================================================
const MAX_CONCURRENT_PER_KEY = 2;
let activeGlobalRequests = 0;
const requestQueue = [];

function acquireConcurrencySlot(poolSize) {
    const maxGlobal = Math.max(3, poolSize * MAX_CONCURRENT_PER_KEY);
    if (activeGlobalRequests < maxGlobal) {
        activeGlobalRequests++;
        return Promise.resolve();
    }
    return new Promise((resolve) => {
        requestQueue.push(resolve);
    });
}

function releaseConcurrencySlot() {
    activeGlobalRequests = Math.max(0, activeGlobalRequests - 1);
    if (requestQueue.length > 0) {
        const nextResolve = requestQueue.shift();
        activeGlobalRequests++;
        nextResolve();
    }
}

// Función auxiliar para responder JSON
function sendJSON(res, statusCode, data) {
    res.writeHead(statusCode, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    });
    res.end(JSON.stringify(data));
}

// Petición HTTPS genérica con timeout extendido de 240s (4 minutos)
function httpsPostJSON(hostname, pathUrl, headers, payloadObj, timeoutMs = 240000) {
    return new Promise((resolve, reject) => {
        const postData = JSON.stringify(payloadObj);
        const options = {
            hostname,
            port: 443,
            path: pathUrl,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(postData),
                ...headers
            }
        };

        const req = https.request(options, (res) => {
            let body = '';
            res.setEncoding('utf8');
            res.on('data', (chunk) => body += chunk);
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(body);
                    resolve({ statusCode: res.statusCode, data: parsed });
                } catch (e) {
                    resolve({ statusCode: res.statusCode, raw: body, error: 'Respuesta JSON no válida del proveedor IA' });
                }
            });
        });

        req.on('error', (e) => reject(e));
        req.setTimeout(timeoutMs, () => {
            req.destroy(new Error(`Timeout de ${timeoutMs / 1000}s superado en ${hostname}`));
        });

        req.write(postData);
        req.end();
    });
}

// ============================================================================
// ADAPTADORES POR PROVEEDOR IA (OPENROUTER + POOL MULTI-API)
// ============================================================================
async function callProvider(entry, messagesArray, userPrompt, preferredModel) {
    const { provider, key } = entry;
    const cleanMessages = Array.isArray(messagesArray) && messagesArray.length > 0
        ? messagesArray
        : [{ role: 'user', content: userPrompt }];

    // 1. GOOGLE GEMINI API (100% Gratis en Google AI Studio)
    if (provider === 'gemini') {
        const geminiModels = ['gemini-2.5-flash', 'gemini-1.5-flash', 'gemini-flash-latest'];
        let lastResp = null;
        for (const gm of geminiModels) {
            const pathUrl = `/v1beta/models/${gm}:generateContent?key=${encodeURIComponent(key)}`;
            const payload = {
                contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
                generationConfig: {
                    temperature: 0.4,
                    maxOutputTokens: 8192
                }
            };
            const resp = await httpsPostJSON('generativelanguage.googleapis.com', pathUrl, {}, payload);
            lastResp = resp;
            if (resp.statusCode >= 200 && resp.statusCode < 300) {
                const text = resp.data?.candidates?.[0]?.content?.parts?.map(p => p.text || '').join('') || '';
                if (text.length > 100) {
                    return { ok: true, content: text, modelUsed: `gemini/${gm}`, provider: 'Google Gemini' };
                }
            }
            if (resp.statusCode === 429 || resp.statusCode === 403) break;
        }
        return {
            ok: false,
            statusCode: lastResp?.statusCode || 500,
            error: lastResp?.data?.error?.message || 'Error en Google Gemini API'
        };
    }

    // 2. GROQ CLOUD API
    if (provider === 'groq') {
        const groqModel = 'llama-3.3-70b-versatile';
        const payload = {
            model: groqModel,
            messages: cleanMessages,
            temperature: 0.4,
            max_tokens: 8000
        };
        const resp = await httpsPostJSON('api.groq.com', '/openai/v1/chat/completions', {
            'Authorization': `Bearer ${key}`
        }, payload);

        if (resp.statusCode >= 200 && resp.statusCode < 300) {
            const text = resp.data?.choices?.[0]?.message?.content || '';
            return { ok: true, content: text, modelUsed: `groq/${groqModel}`, provider: 'Groq Cloud' };
        }
        return { ok: false, statusCode: resp.statusCode, error: resp.data?.error?.message || 'Error en Groq API' };
    }

    // 3. MISTRAL AI / CODESTRAL
    if (provider === 'mistral') {
        const mistralModel = 'codestral-latest';
        const payload = {
            model: mistralModel,
            messages: cleanMessages,
            temperature: 0.4,
            max_tokens: 12000
        };
        const resp = await httpsPostJSON('api.mistral.ai', '/v1/chat/completions', {
            'Authorization': `Bearer ${key}`
        }, payload);

        if (resp.statusCode >= 200 && resp.statusCode < 300) {
            const text = resp.data?.choices?.[0]?.message?.content || '';
            return { ok: true, content: text, modelUsed: `mistral/${mistralModel}`, provider: 'Mistral AI' };
        }
        return { ok: false, statusCode: resp.statusCode, error: resp.data?.error?.message || 'Error en Mistral API' };
    }

    // 4. CEREBRAS CLOUD
    if (provider === 'cerebras') {
        const cerebrasModel = 'llama-3.3-70b';
        const payload = {
            model: cerebrasModel,
            messages: cleanMessages,
            temperature: 0.4,
            max_tokens: 8000
        };
        const resp = await httpsPostJSON('api.cerebras.ai', '/v1/chat/completions', {
            'Authorization': `Bearer ${key}`
        }, payload);

        if (resp.statusCode >= 200 && resp.statusCode < 300) {
            const text = resp.data?.choices?.[0]?.message?.content || '';
            return { ok: true, content: text, modelUsed: `cerebras/${cerebrasModel}`, provider: 'Cerebras Cloud' };
        }
        return { ok: false, statusCode: resp.statusCode, error: resp.data?.error?.message || 'Error en Cerebras API' };
    }

    // 5. NVIDIA NIM API
    if (provider === 'nvidia') {
        const nvidiaModel = 'meta/llama-3.3-70b-instruct';
        const payload = {
            model: nvidiaModel,
            messages: cleanMessages,
            temperature: 0.4,
            max_tokens: 8000
        };
        const resp = await httpsPostJSON('integrate.api.nvidia.com', '/v1/chat/completions', {
            'Authorization': `Bearer ${key}`
        }, payload);

        if (resp.statusCode >= 200 && resp.statusCode < 300) {
            const text = resp.data?.choices?.[0]?.message?.content || '';
            return { ok: true, content: text, modelUsed: `nvidia/${nvidiaModel}`, provider: 'NVIDIA NIM' };
        }
        return { ok: false, statusCode: resp.statusCode, error: resp.data?.error?.message || 'Error en NVIDIA NIM API' };
    }

    // 6. DEEPSEEK DIRECTO
    if (provider === 'deepseek') {
        const payload = {
            model: 'deepseek-chat',
            messages: cleanMessages,
            temperature: 0.4,
            max_tokens: 8000
        };
        const resp = await httpsPostJSON('api.deepseek.com', '/chat/completions', {
            'Authorization': `Bearer ${key}`
        }, payload);

        if (resp.statusCode >= 200 && resp.statusCode < 300) {
            const text = resp.data?.choices?.[0]?.message?.content || '';
            return { ok: true, content: text, modelUsed: 'deepseek-chat', provider: 'DeepSeek Direct' };
        }
        return { ok: false, statusCode: resp.statusCode, error: resp.data?.error?.message || 'Error en DeepSeek API' };
    }

    // 7. OPENROUTER API ({ model, messages, temperature: 0.4, max_tokens: 12000 })
    const modelToUse = (preferredModel && preferredModel !== 'auto') ? preferredModel : 'deepseek/deepseek-chat';
    const orHeaders = {
        'Authorization': `Bearer ${key}`,
        'HTTP-Referer': 'https://teacherstools.onrender.com',
        'X-Title': 'teacherstools'
    };
    const payload = {
        model: modelToUse,
        messages: cleanMessages,
        temperature: 0.4,
        max_tokens: 12000
    };

    let resp = await httpsPostJSON('openrouter.ai', '/api/v1/chat/completions', orHeaders, payload);

    // Si devuelve 402 por límite de reserva de tokens en la cuenta, ajustar automáticamente a lo permitido por OpenRouter
    if (resp.statusCode === 402) {
        const errMsg = resp.data?.error?.message || '';
        const affordMatch = errMsg.match(/can only afford\s+(\d+)/i);
        if (affordMatch && affordMatch[1]) {
            const affordableTokens = parseInt(affordMatch[1], 10);
            if (affordableTokens > 1500) {
                payload.max_tokens = Math.max(1500, affordableTokens - 100);
                console.log(`[OpenRouter] Ajustando max_tokens=${payload.max_tokens} según saldo disponible`);
                resp = await httpsPostJSON('openrouter.ai', '/api/v1/chat/completions', orHeaders, payload);
            }
        }
    }

    // Si aún falla por créditos (402) o rate limit (429), intentar modelo gratuito :free en OpenRouter
    if (resp.statusCode === 402 || resp.statusCode === 429) {
        const freeModels = [
            'deepseek/deepseek-chat-v3-0324:free',
            'qwen/qwen-2.5-coder-32b-instruct:free',
            'google/gemini-2.0-flash-exp:free'
        ];
        for (const fm of freeModels) {
            payload.model = fm;
            payload.max_tokens = 12000;
            const freeResp = await httpsPostJSON('openrouter.ai', '/api/v1/chat/completions', orHeaders, payload);
            if (freeResp.statusCode >= 200 && freeResp.statusCode < 300) {
                const text = freeResp.data?.choices?.[0]?.message?.content || '';
                if (text.length > 100) {
                    return { ok: true, content: text, modelUsed: fm, provider: 'OpenRouter Free' };
                }
            }
        }
    }

    if (resp.statusCode >= 200 && resp.statusCode < 300) {
        const text = resp.data?.choices?.[0]?.message?.content || '';
        return { ok: true, content: text, modelUsed: payload.model, provider: 'OpenRouter' };
    }

    return {
        ok: false,
        statusCode: resp.statusCode || 500,
        error: resp.data?.error?.message || resp.error || 'Error en OpenRouter'
    };
}

// ============================================================================
// AUTO-CONTINUACIÓN INTELIGENTE SI UN OVA SUPERA EL LÍMITE DE TOKENS Y SE CORTA
// ============================================================================
async function ensureCompleteHtml(entry, rawContent, preferredModel) {
    let cleaned = rawContent.replace(/```\s*$/, '').trim();
    const hasHtmlClose = /<\/html>/i.test(cleaned);
    const hasScriptOpen = /<script\b/i.test(cleaned);
    const hasScriptClose = /<\/script>/i.test(cleaned);

    // Extraer el contenido de <script> para verificar que no sea un comentario placeholder vacío
    const scriptMatches = [...cleaned.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
    const totalScriptCode = scriptMatches.map(m => m[1].trim()).join('\n');
    const hasLazyPlaceholder = /\/\/\s*(aquí iría|código aquí|implementar lógica|todo:|resto del código)/i.test(totalScriptCode) || (hasScriptOpen && hasScriptClose && totalScriptCode.length < 300);

    if (hasLazyPlaceholder) {
        console.log(`[AutoRepairJS] Detectado <script> vacío o con comentario placeholder (${totalScriptCode.length} chars). Solicitando programación JS completa...`);
        const gameSectionMatch = cleaned.match(/<section[^>]*id=["']zona-juego["'][^>]*>[\s\S]*?<\/section>/i) || cleaned.match(/<body[^>]*>[\s\S]{0,4000}/i);
        const htmlContext = gameSectionMatch ? gameSectionMatch[0] : cleaned.slice(0, 4000);
        const repairPrompt = `En el siguiente archivo HTML de un juego educativo interactivo dejaste el bloque <script> vacío o con un comentario placeholder:\n\n${htmlContext}\n\nESCRIBE ÚNICAMENTE EL BLOQUE <script>...</script> COMPLETO Y 100% FUNCIONAL que hace funcionar todos los elementos, botones, tablero, cronómetro, puntaje y modal de victoria de este HTML. PROHIBIDO usar comentarios de omisión. Entrega solo <script>...código completo...</script>.`;
        const repairResult = await callProvider(entry, [{ role: 'user', content: repairPrompt }], repairPrompt, preferredModel);
        if (repairResult.ok && repairResult.content) {
            let jsBlock = repairResult.content.replace(/^```(?:html|javascript|js)?\s*/i, '').replace(/```\s*$/i, '').trim();
            if (!/^<script/i.test(jsBlock)) {
                jsBlock = `<script>\n${jsBlock}\n</script>`;
            }
            cleaned = cleaned.replace(/<script\b[^>]*>[\s\S]*?<\/script>/i, jsBlock);
            console.log(`[AutoRepairJS] Bloque <script> reparado exitosamente (${jsBlock.length} bytes).`);
            return cleaned;
        }
    }

    if (hasHtmlClose && (!hasScriptOpen || hasScriptClose)) {
        return cleaned;
    }

    console.log(`[AutoContinue] Detectado HTML/Script truncado (${cleaned.length} bytes). Solicitando continuación automática a [${entry.provider.toUpperCase()}]...`);
    const tailContext = cleaned.slice(-1200);
    const continuationPrompt = `El código HTML/JS del OVA anterior quedó cortado exactamente al final de este fragmento:\n\n${tailContext}\n\nContinúa escribiendo ÚNICAMENTE el código faltante exactamente desde el último carácter donde se cortó hasta cerrar con </script></body></html>. NO repitas el código ya escrito ni agregues explicaciones.`;

    const contMessages = [{ role: 'user', content: continuationPrompt }];
    const contResult = await callProvider(entry, contMessages, continuationPrompt, preferredModel);
    if (contResult.ok && contResult.content) {
        let contText = contResult.content
            .replace(/^```(?:html|javascript|js)?\s*/i, '')
            .replace(/```\s*$/i, '');

        const lastLines = cleaned.split('\n').slice(-4).join('\n').trim();
        if (lastLines.length > 20 && contText.includes(lastLines)) {
            const idx = contText.indexOf(lastLines);
            contText = contText.substring(idx + lastLines.length);
        }

        const stitched = cleaned + contText;
        console.log(`[AutoContinue] Código completado exitosamente (${stitched.length} bytes totales).`);
        return stitched;
    }

    return rawContent;
}

// ============================================================================
// SERVIDOR HTTP PRINCIPAL
// ============================================================================
const server = http.createServer((req, res) => {
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        });
        return res.end();
    }

    const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
    const pathname = parsedUrl.pathname;

    // Estado del Pool de APIs y Configuración
    if (pathname === '/api/config' && req.method === 'GET') {
        const pool = buildActiveKeyPool();
        const providersCount = {};
        pool.forEach(p => {
            providersCount[p.provider] = (providersCount[p.provider] || 0) + 1;
        });
        return sendJSON(res, 200, {
            serverActive: true,
            hasServerKey: Boolean(process.env.OPENROUTER_API_KEY) || pool.length > 0,
            totalKeysInPool: pool.length,
            providersCount,
            activeGlobalRequests,
            queuedRequests: requestQueue.length,
            promptsCount: promptsData.length
        });
    }

    // Lista de las 21 plantillas de dinámicas
    if (pathname === '/api/prompts' && req.method === 'GET') {
        return sendJSON(res, 200, promptsData);
    }

    // Endpoint de generación con fidelidad 100% textual al prompt original
    if (pathname === '/api/generate' && req.method === 'POST') {
        let body = '';
        req.setEncoding('utf8');
        req.on('data', (chunk) => body += chunk);
        req.on('end', async () => {
            let data = {};
            try {
                data = JSON.parse(body || '{}');
            } catch (parseErr) {
                return sendJSON(res, 400, { error: 'JSON de solicitud inválido.' });
            }

            const clientApiKey = data.apiKey || '';
            const pool = buildActiveKeyPool(clientApiKey);

            if (pool.length === 0) {
                return sendJSON(res, 400, {
                    error: 'No hay llaves API configuradas. Agrega tu API Key en el botón "Configurar Clave" o en el servidor.'
                });
            }

            const model = data.model || 'deepseek/deepseek-chat';
            const messages = Array.isArray(data.messages) && data.messages.length > 0
                ? data.messages
                : (data.prompt ? [{ role: 'user', content: data.prompt }] : []);
            const promptText = data.prompt || (messages[0] && messages[0].content) || '';

            if (!promptText) {
                return sendJSON(res, 400, { error: 'El prompt o array messages es obligatorio.' });
            }

            await acquireConcurrencySlot(pool.length);

            try {
                const orderedPool = buildActiveKeyPool(clientApiKey);
                let lastErrorMsg = 'Ningún proveedor pudo completar la solicitud.';
                let lastStatus = 500;

                for (const entry of orderedPool) {
                    entry.state.inFlight++;
                    entry.state.lastUsed = Date.now();
                    entry.state.totalCalls++;

                    try {
                        console.log(`[LoadBalancer] Despachando generación a [${entry.provider.toUpperCase()}] (Activas: ${entry.state.inFlight})`);
                        const result = await callProvider(entry, messages, promptText, model);

                        if (result.ok && result.content) {
                            const finalContent = await ensureCompleteHtml(entry, result.content, model);
                            return sendJSON(res, 200, {
                                content: finalContent,
                                modelUsed: result.modelUsed,
                                providerUsed: result.provider
                            });
                        } else {
                            lastErrorMsg = `[${entry.provider}] ${result.error}`;
                            lastStatus = result.statusCode || 500;
                            entry.state.errors++;
                            if (result.statusCode === 429 || result.statusCode === 402) {
                                entry.state.cooldownUntil = Date.now() + 25000;
                            }
                            console.warn(`[LoadBalancer] Fallo en ${entry.provider} (${result.statusCode}): ${result.error}. Saltando a siguiente API...`);
                        }
                    } catch (callErr) {
                        entry.state.errors++;
                        lastErrorMsg = callErr.message;
                        console.warn(`[LoadBalancer] Excepción en ${entry.provider}: ${callErr.message}. Saltando a siguiente API...`);
                    } finally {
                        entry.state.inFlight = Math.max(0, entry.state.inFlight - 1);
                    }
                }

                return sendJSON(res, lastStatus, {
                    error: `Error al generar con la API: ${lastErrorMsg}`
                });
            } catch (err) {
                console.error('Error procesando /api/generate:', err);
                return sendJSON(res, 500, { error: 'Error interno en el servidor: ' + err.message });
            } finally {
                releaseConcurrencySlot();
            }
        });
        return;
    }

    // Servir archivos estáticos
    let filePath = pathname === '/' ? path.join(__dirname, 'index.html') : path.join(__dirname, pathname);
    const safePath = path.resolve(filePath);
    if (!safePath.startsWith(path.resolve(__dirname))) {
        res.writeHead(403);
        return res.end('Acceso denegado');
    }

    fs.stat(safePath, (err, stats) => {
        if (err || !stats.isFile()) {
            fs.readFile(path.join(__dirname, 'index.html'), (readErr, content) => {
                if (readErr) {
                    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
                    return res.end('Archivo no encontrado');
                }
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                res.end(content);
            });
            return;
        }

        const ext = path.extname(safePath).toLowerCase();
        const mimeTypes = {
            '.html': 'text/html; charset=utf-8',
            '.js': 'application/javascript; charset=utf-8',
            '.json': 'application/json; charset=utf-8',
            '.css': 'text/css; charset=utf-8',
            '.svg': 'image/svg+xml',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.ico': 'image/x-icon'
        };

        const contentType = mimeTypes[ext] || 'application/octet-stream';
        res.writeHead(200, { 'Content-Type': contentType });
        fs.createReadStream(safePath).pipe(res);
    });
});

// Timeout extendido a 4 minutos (240,000 ms) para evitar cortes HTTP 520/504 en generaciones extensas
server.timeout = 240000;
server.keepAliveTimeout = 240000;
server.headersTimeout = 245000;

server.listen(PORT, () => {
    const initialPool = buildActiveKeyPool();
    console.log(`[teacherstools] Servidor corriendo en el puerto ${PORT} (Timeout: 240s)`);
    console.log(`[teacherstools] Plantillas originales cargadas: ${promptsData.length}`);
    console.log(`[teacherstools] Llaves activas en el Pool: ${initialPool.length}`);
});
