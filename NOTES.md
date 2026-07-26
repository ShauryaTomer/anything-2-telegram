# Teaching Notes

- User: engineer with backend, networking, and HTTP-method knowledge.
- Teach runtime mechanics; avoid beginner HTTP/networking review.
- Prefer concise Markdown and Mermaid diagrams.
- Current target: understand pyee `AsyncIOEventEmitter` and Observer deeply enough to review implementation.
- PyPubSub lessons remain as comparison material; implementation direction switched to pyee.
- User reasons well about design smells unprompted (see record 0003). Surface tensions between a proposed design and their own prior conclusions rather than smoothing them over — they want the conflict named.
- Lessons from 0006 on may use Mermaid via `assets/diagram.js` (palette-matched from CSS vars, zoom/pan, graceful fallback). All styling stays in `assets/lesson.css` so every lesson reads as one course.
- Three traps found the hard way, all recorded as comments in the assets:
  - `diagram.js` must be a **classic** script. Lessons are opened off disk, and `file://` pages have a null origin, so ES-module CDN imports are CORS-blocked and diagrams silently stay raw text.
  - Zoom uses CSS `zoom`, not `transform: scale()` — transform leaves the layout box unscaled, clipping tall diagrams.
  - `.node` is scoped to `.flow .node`; a bare `.node` rule leaks into Mermaid's internal SVG `.node` groups.
- User dislikes bland output. Lesson typography is Instrument Serif / Crimson Pro / JetBrains Mono with a warm paper palette; stat rows and weighted cards carry the key numbers and comparisons.
