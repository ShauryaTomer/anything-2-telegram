# Vendored assets

Downloaded verbatim, served locally so the page needs no external network access.

| file | source | sha384 |
| --- | --- | --- |
| `htmx-2.0.9.min.js` | https://cdn.jsdelivr.net/npm/htmx.org@2.0.9/dist/htmx.min.js | `ESlCao+z/oasnu2Uc/5K1LQTI7YCF2KKO4xakCPQCFuiHhCh8Oa/R5NwHY6guZ3m` |
| `pico-2.1.1.classless.min.css` | https://cdn.jsdelivr.net/npm/@picocss/pico@2.1.1/css/pico.classless.min.css | `NZhm4G1I7BpEGdjDKnzEfy3d78xvy7ECKUwwnKTYi036z42IyF056PbHfpQLIYgL` |

Re-vendor with:

```
curl -L https://cdn.jsdelivr.net/npm/htmx.org@2.0.9/dist/htmx.min.js \
  -o anything2telegram/web/static/vendor/htmx-2.0.9.min.js
curl -L https://cdn.jsdelivr.net/npm/@picocss/pico@2.1.1/css/pico.classless.min.css \
  -o anything2telegram/web/static/vendor/pico-2.1.1.classless.min.css
openssl dgst -sha384 -binary <file> | openssl base64 -A   # verify against the table above
```
