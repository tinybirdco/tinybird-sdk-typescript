# Tinybird SDK SQL Highlighting Extension

Inject SQL + Jinja-like syntax highlighting into TypeScript/TSX strings used by the Tinybird TypeScript SDK.

## How It Works

The extension injects a SQL grammar into TypeScript and TSX when it detects Tinybird SDK SQL fields.  
It targets string values assigned to:

- `sql`
- `forwardQuery`
- `partitionKey`
- `ttl`

Inside those strings, it highlights:

- SQL keywords, functions, types, comments, and numbers
- Tinybird/Jinja template tags like `{{ ... }}` and `{% ... %}`

## Example

```bash
node({
  sql: `
    SELECT count()
    FROM events
    WHERE ts >= {{ DateTime(start_date) }}
  `,
})
```
