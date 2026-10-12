const http = require('http');
const fs = require('fs');
const path = require('path');
const https = require('https');

const PORT = process.env.PORT || 3000;
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || '';

// Cargar prompts preprocesados
let promptsData = [];
try {
    const rawData = fs.readFileSync(path.join(__dirname, 'prompts_data.json'), 'utf8');
    promptsData = JSON.parse(rawData);
} catch (err) {
    console.error('Error cargando prompts_data.json:', err.message);
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

// Llamada HTTPS a OpenRouter
function callOpenRouter(apiKey, payload) {
    return new Promise((resolve, reject) => {
        const postData = JSON.stringify(payload);
        const options = {
            hostname: 'openrouter.ai',
            port: 443,
            path: '/api/v1/chat/completions',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`,
                'HTTP-Referer': 'https://teacherstools.onrender.com',
                'X-Title': 'teacherstools',
                'Content-Length': Buffer.byteLength(postData)
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
                    resolve({ statusCode: res.statusCode, raw: body, error: 'Respuesta no válida de OpenRouter' });
                }
            });
        });

        req.on('error', (e) => reject(e));
        req.setTimeout(120000, () => {
            req.destroy(new Error('Timeout de 120s superado al conectar con OpenRouter'));
        });

        req.write(postData);
        req.end();
    });
}

const server = http.createServer((req, res) => {
    // Manejo de CORS preflight
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

    // Ruta de estado y configuración
    if (pathname === '/api/config' && req.method === 'GET') {
        return sendJSON(res, 200, {
            hasServerKey: Boolean(OPENROUTER_API_KEY && OPENROUTER_API_KEY.trim().length > 0),
            serverActive: true,
            promptsCount: promptsData.length
        });
    }

    // Ruta para obtener la lista de prompts
    if (pathname === '/api/prompts' && req.method === 'GET') {
        return sendJSON(res, 200, promptsData);
    }

    // Endpoint de generación
    if (pathname === '/api/generate' && req.method === 'POST') {
        let body = '';
        req.setEncoding('utf8');
        req.on('data', (chunk) => body += chunk);
        req.on('end', async () => {
            try {
                const data = JSON.parse(body || '{}');
                const clientApiKey = data.apiKey;
                const effectiveKey = clientApiKey || OPENROUTER_API_KEY;

                if (!effectiveKey) {
                    return sendJSON(res, 400, {
                        error: 'No se encontró API Key de OpenRouter. Configura OPENROUTER_API_KEY en las variables de entorno del servidor o proporciona una clave personal en el panel de configuración.'
                    });
                }

                const model = data.model || 'deepseek/deepseek-chat';
                const promptText = data.prompt;

                if (!promptText) {
                    return sendJSON(res, 400, { error: 'El campo "prompt" es obligatorio.' });
                }

                const defaultSystemInstruction = `Actúa como un Desarrollador Web Front-End Senior, Diseñador UI/UX Educativo y Diseñador Instruccional del Instituto Montenegro.
Tu misión es generar ÚNICAMENTE el código HTML completo, interactivo, colorido, profesional y 100% OFFLINE del Objeto Virtual de Aprendizaje (OVA) solicitado, siguiendo de manera estricta la arquitectura CSS, HTML y JavaScript especificada en el prompt del usuario.

ESTÁNDARES DE CALIDAD VISUAL Y FUNCIONALIDAD (INSTITUTO MONTENEGRO):
1. ZONA DE JUEGO (#zona-juego) DE ALTO IMPACTO:
   - Implementa exactamente los estilos oscuros inmersivos (#111827 / #0f172a) o encabezados con degradados vibrantes (linear-gradient) indicados en la plantilla de la dinámica.
   - Incluye la barra de estado (.status-bar / .stats) con el cronómetro '⏱️ Tiempo: MM:SS / 10:00', contador de aciertos/rondas, errores y calificación dinámica de 1.0 a 5.0.
   - Incorpora iconografía SVG inline, botones y píldoras redondeadas (border-radius: 50px), y animaciones CSS (@keyframes popOut, popIn, shakeError).
   - Aplica 'touch-action: none' y 'user-select: none' en los contenedores táctiles del juego.
2. FASE 3 EDITORIAL (#zona-lectura) SIEMPRE VISIBLE DEBAJO DEL JUEGO:
   - Contenedor claro (#f8fafc) con tarjeta '.article-container' (fondo blanco, border-radius: 16px, max-width: 850px), subtítulos h2 con borde inferior esmeralda (#059669), h3 con borde izquierdo (#4ade80) y caja '.highlight-box' (#ecfdf5).
   - Sección '.workshop-section' con '.instruction-box' amarilla punteada y lista numerada '.question-list' de 10 tarjetas '.question-item', cada una con su etiqueta '.badge' coloreada (.badge-answer azul #3b82f6, .badge-draw verde #10b981, .badge-critical rojo #ef4444, .badge-map morado #8b5cf6).
3. CÓDIGO 100% FUNCIONAL Y SIN OMISIONES:
   - Entrega ÚNICAMENTE el bloque \`\`\`html ... \`\`\` completo desde <!DOCTYPE html> hasta </html>, sin texto conversacional ni bloques truncados.`;

                const effectiveSystemInstruction = data.systemPrompt || defaultSystemInstruction;

                const openRouterPayload = {
                    model: model,
                    messages: [
                        { role: 'system', content: effectiveSystemInstruction },
                        { role: 'user', content: promptText }
                    ],
                    temperature: 0.3,
                    max_tokens: 8000
                };

                let response = await callOpenRouter(effectiveKey, openRouterPayload);

                // Si OpenRouter devuelve 402 por límite de créditos en max_tokens, ajustar automáticamente y reintentar
                if (response.statusCode === 402) {
                    const errMsg = response.data?.error?.message || '';
                    const affordMatch = errMsg.match(/can only afford\s+(\d+)/i);
                    if (affordMatch && affordMatch[1]) {
                        const affordableTokens = parseInt(affordMatch[1], 10);
                        if (affordableTokens > 1500) {
                            openRouterPayload.max_tokens = Math.max(1500, affordableTokens - 150);
                            console.log(`[teacherstools] Reintentando automáticamente con max_tokens=${openRouterPayload.max_tokens}`);
                            response = await callOpenRouter(effectiveKey, openRouterPayload);
                        }
                    }
                }

                if (response.statusCode >= 200 && response.statusCode < 300) {
                    const messageContent = response.data.choices?.[0]?.message?.content || '';
                    return sendJSON(res, 200, {
                        content: messageContent,
                        modelUsed: model,
                        usage: response.data.usage || null
                    });
                } else {
                    console.error('Error OpenRouter:', response);
                    return sendJSON(res, response.statusCode || 500, {
                        error: response.data?.error?.message || response.error || 'Error al comunicarse con OpenRouter.',
                        raw: response.data
                    });
                }
            } catch (err) {
                console.error('Error procesando /api/generate:', err);
                return sendJSON(res, 500, { error: 'Error interno en el servidor: ' + err.message });
            }
        });
        return;
    }

    // Servir archivos estáticos
    let filePath = pathname === '/' ? path.join(__dirname, 'index.html') : path.join(__dirname, pathname);

    // Evitar Path Traversal
    const safePath = path.resolve(filePath);
    if (!safePath.startsWith(path.resolve(__dirname))) {
        res.writeHead(403);
        return res.end('Acceso denegado');
    }

    fs.stat(safePath, (err, stats) => {
        if (err || !stats.isFile()) {
            // Fallback a index.html para SPA
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

server.listen(PORT, () => {
    console.log(`[teacherstools] Servidor corriendo exitosamente en el puerto ${PORT}`);
    console.log(`[teacherstools] Clave OpenRouter en servidor: ${OPENROUTER_API_KEY ? 'CONFIGURADA (OK)' : 'NO CONFIGURADA'}`);
});
