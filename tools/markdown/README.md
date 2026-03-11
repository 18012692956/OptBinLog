# Markdown Tools

## 1) Edit in VS Code

- Open a file: `code 毕业论文初稿.md`
- Open preview: `Cmd + Shift + V`
- Side preview: `Cmd + K` then `V`

Workspace style is configured in:

- `.vscode/settings.json`
- `docs/assets/markdown-preview.css`

## 2) Read in GUI

- Use `tools/markdown/md-open <file.md>`
- It prefers `Obsidian`, then falls back to `MarkText`, then `VS Code`.

## 3) Read in Terminal

- Use `tools/markdown/md-read <file.md> [width]`
- Example: `tools/markdown/md-read 毕业论文初稿.md 100`
