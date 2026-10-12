# teacherstools 🎓

**Suite y Generador Profesional de Objetos Virtuales de Aprendizaje (OVAs) Gamificados 100% Offline para Docentes.**

`teacherstools` transforma cualquier tema curricular y grado académico en interactivos web autónomos y pedagógicos listos para producción y aprendizaje activo.

---

## ✨ Características Principales

1. **21 Dinámicas Gamificadas Maestras:**
   - **Palabras y Vocabulario:** Sopa de Letras, Crucigrama, Completar Párrafo, Anagrama, Ahorcado (Misión Rescate), Criptograma Científico.
   - **Memoria y Asociación:** Emparejar, Concéntrese (Memoria Idéntica), Clasificador Tap & Sort, Hotspots (Etiquetar Diagrama), Tarjetas Tinder (Verdadero/Falso).
   - **Pensamiento Lógico y Secuencia:** Laberinto de Decisiones, Escape Room, Ordenar Secuencia, Rompecabezas de Frases.
   - **Trivia y Arcade:** Lluvia de Conceptos, Trivia Contra Reloj (Racha), Ruleta del Saber, Jeopardy.
   - **Música e Inclusión Especial:** Simulador de Piano Acústico EpC (Web Audio ADSR de 2 octavas), Lectoescritura Adaptada / Neuroinclusión (Lectura fácil, modo sílabas, voz Web Speech, foco de renglón).

2. **100% Offline (Modo Escuela Rural y Colegio):**
   - El código generado tiene **CERO dependencias de CDNs**, librerías externas o internet.
   - Tipografía nativa del sistema (`system-ui`), iconografía SVG pura, y APIs nativas del navegador (Web Audio API y Web Speech API).
   - Diseñado con `touch-action: none` y tipografía elástica `clamp()` para celulares, tabletas y computadores.

3. **Fase 3 Estricta Visible:**
   - Todo OVA incluye obligatoriamente un **artículo teórico profundo (>500 palabras)** y un **Taller de 10 actividades para el cuaderno**, alternando entre responder, analizar críticamente, dibujar esquemas y elaborar mapas conceptuales.

4. **Conexión Inteligente con OpenRouter:**
   - Detección automática de la variable de entorno `OPENROUTER_API_KEY` en Render.
   - Opcionalmente, permite ingresar una clave personal en el navegador (`localStorage`).
   - Selección de modelos de IA de alta precisión como Claude 3.5 Sonnet, Gemini 2.5 Pro o DeepSeek.

---

## 🚀 Despliegue en Render (con Blueprint)

Esta aplicación incluye el archivo `render.yaml` listo para despliegue automatizado.

### Pasos:
1. Sube este repositorio a tu cuenta de GitHub.
2. Inicia sesión en [Render.com](https://render.com).
3. Haz clic en **New +** y selecciona **Blueprint**.
4. Conecta tu repositorio de GitHub `teacherstools`.
5. En la variable de entorno `OPENROUTER_API_KEY`, pega tu clave de OpenRouter (`sk-or-v1-...`).
6. Haz clic en **Apply**. ¡Render compilará e iniciará el servicio de inmediato!

---

## 💻 Ejecución Local

```bash
# Instalar (no requiere paquetes pesados, utiliza Node.js nativo)
npm install

# Iniciar servidor
npm start
```

Abre tu navegador en `http://localhost:3000`.

---

## 📄 Licencia

Distribuido bajo la Licencia MIT. Creado para potenciar la educación de calidad.
