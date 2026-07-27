function ts() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

function log(level, msg, extra) {
  const line = [`[${ts()}]`, `[${level}]`, msg];
  if (extra) line.push(JSON.stringify(extra));
  console.log(line.join(" "));
}

export const logger = {
  info: (m, e) => log("info", m, e),
  warn: (m, e) => log("warn", m, e),
  error: (m, e) => log("error", m, e),
  stream: (m, e) => log("stream", m, e),
  push: (m, e) => log("push", m, e),
};
