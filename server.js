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

                const defaultSystemInstruction = `Actúa como un Desarrollador Web Front-End Senior, Diseñador UI/UX Educativo y Diseñador Instruccional de clase mundial.
Tu misión es generar ÚNICAMENTE el código HTML completo, interactivo, altamente visual, colorido, profesional y 100% OFFLINE del Objeto Virtual de Aprendizaje (OVA) solicitado.

REGLAS DE DISEÑO VISUAL Y ESTÉTICA PREMIUM (OBLIGATORIAS):
1. PALETA DE COLORES VIBRANTE Y TEMÁTICA:
   - Define variables CSS (:root) con una paleta cromática armónica, rica y temática al contenido (ej. azul profundo y esmeralda para biología/ecosistemas, púrpura y magenta para matemáticas/lógica, ámbar y naranja cálido para historia).
   - Fondos suaves degradados o contrastados (evita fondos blancos planos y aburridos; usa fondos sutiles como #f1f8e9, #f0fdf4 o #eff6ff con tarjetas blancas limpias).
2. ENCABEZADO IMPACTANTE:
   - Header con degradado CSS fluido (linear-gradient), tipografía elegante, sombras y detalles SVG decorativos sutiles.
3. TARJETAS, HUD Y COMPONENTES MODERNOS:
   - Contenedores con border-radius generoso (14px a 20px), sombras pulidas (box-shadow: 0 10px 25px rgba(0,0,0,0.08)) y bordes de acento de 4-6px.
   - HUD superior con cronómetro de práctica, metas, medidores de vidas/puntos, badges redondeados tipo píldora (pill badges).
   - Animaciones y microinteracciones CSS fluidas: transiciones en hover, popIn al acertar, shake suave al fallar, estados activos luminosos y modales de victoria con confeti SVG o animaciones de celebración.
4. FASE 3 DE ALTA CALIDAD EDITORIAL:
   - Artículo estructurado tipo revista científica o enciclopedia escolar moderna: subtítulos h3 con íconos o viñetas temáticas, cajas destacadas (.info-box) para conceptos clave, tipografía con line-height amplio y taller de 10 actividades en una tarjeta (.workshop) visualmente atractiva con llamadas a la acción para el cuaderno.

REGLAS TÉCNICAS ESTRICTAS:
1. Responde entregando ÚNICAMENTE el código HTML completo y autocontenido dentro de un solo bloque de código \`\`\`html ... \`\`\`.
2. NO incluyas introducciones, ni saludos, ni explicaciones posteriores.
3. 100% OFFLINE: CERO enlaces a CDNs (sin Tailwind externo, sin Google Fonts, sin Bootstrap). Usa tipografías del sistema (system-ui) e iconos SVG puros incrustados.
4. JavaScript 100% implementado, interactivo y sin omisiones de código (cero '// implementar aquí').
5. FASE 3 visible debajo del juego con artículo (>500 palabras) y taller de 10 actividades.
6. Temporizador con meta de práctica de 10:00 minutos y candado de finalización reflexiva.`;

                const effectiveSystemInstruction = data.systemPrompt || defaultSystemInstruction;

                const openRouterPayload = {
                    model: model,
                    messages: [
                        { role: 'system', content: effectiveSystemInstruction },
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
