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

                const model = data.model || 'anthropic/claude-3.5-sonnet';
                const promptText = data.prompt;

                if (!promptText) {
                    return sendJSON(res, 400, { error: 'El campo "prompt" es obligatorio.' });
                }

                const systemInstruction = `Actúa como un Desarrollador Web Front-End Senior y Diseñador Instruccional de clase mundial.
Tu misión es generar ÚNICAMENTE el código HTML completo, interactivo, profesional y 100% OFFLINE del Objeto Virtual de Aprendizaje (OVA) solicitado.

REGLAS ESTRICTAS DE RESPUESTA:
1. Responde entregando ÚNICAMENTE el código HTML completo y autocontenido dentro de un solo bloque de código \`\`\`html ... \`\`\`.
2. NO incluyas introducciones, ni saludos, ni explicaciones posteriores.
3. El archivo debe ser 100% OFFLINE: CERO enlaces a CDNs (sin Tailwind externo, sin Google Fonts, sin Bootstrap, sin FontAwesome). Usa tipografías estándar del sistema operativo y dibuja iconos usando código SVG inline.
4. Todo el código JavaScript debe estar completamente implementado dentro de <script>. ESTÁ ESTRICTAMENTE PROHIBIDO dejar funciones vacías, usar '// implementar aquí' o comentarios de omisión. El OVA debe ser jugable e interactivo de inmediato.
5. Incluye siempre la FASE 3 estáticamente visible debajo del juego con lectura teórica profunda (>500 palabras) y el taller de 10 actividades para el cuaderno alternando formatos didácticos.`;

                const openRouterPayload = {
                    model: model,
                    messages: [
                        { role: 'system', content: systemInstruction },
                        { role: 'user', content: promptText }
                    ],
                    temperature: 0.3,
                    max_tokens: 16000
                };

                const response = await callOpenRouter(effectiveKey, openRouterPayload);

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
