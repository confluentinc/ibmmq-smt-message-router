# SMT Routing Diagram

This directory contains a Mermaid diagram showing how SMT-based routing works.

## smt-routing.mmd

Visualizes the flow:
1. MQ message with properties
2. Connector converts properties to Kafka headers
3. RegexRouter SMT reads header and determines topic
4. Message routes to appropriate Kafka topic

## Viewing the Diagram

### On GitHub
The `.mmd` file renders automatically when viewing on GitHub.

### Mermaid Live Editor
1. Go to https://mermaid.live/
2. Copy the contents of `smt-routing.mmd`
3. Paste into the editor to see the rendered diagram

### Export to Image
```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i smt-routing.mmd -o smt-routing.png
```
