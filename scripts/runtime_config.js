const fs = require("fs");
const path = require("path");
const https = require("https");

let envLoaded = false;
let cachedAgent = null;
const DEFAULT_TIMEZONE = "America/Guayaquil";

function parseEnvLine(line) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) {
    return null;
  }
  const idx = trimmed.indexOf("=");
  if (idx < 0) {
    return null;
  }
  const key = trimmed.slice(0, idx).trim();
  const value = trimmed.slice(idx + 1).trim();
  if (!key) {
    return null;
  }
  return { key, value };
}

function loadEnvFile() {
  if (envLoaded) {
    return;
  }
  envLoaded = true;
  const envPath = path.resolve(".env");
  if (!fs.existsSync(envPath)) {
    return;
  }
  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    const pair = parseEnvLine(line);
    if (!pair) {
      continue;
    }
    if (process.env[pair.key] === undefined) {
      process.env[pair.key] = pair.value;
    }
  }
}

function isTrue(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function getHttpsAgent() {
  if (cachedAgent) {
    return cachedAgent;
  }
  loadEnvFile();
  const insecure = isTrue(process.env.COBERTURA_TLS_INSECURE);
  if (insecure) {
    cachedAgent = new https.Agent({ rejectUnauthorized: false });
    return cachedAgent;
  }

  const caFile = process.env.COBERTURA_CA_FILE ? path.resolve(process.env.COBERTURA_CA_FILE) : "";
  if (!caFile) {
    cachedAgent = new https.Agent({ rejectUnauthorized: true });
    return cachedAgent;
  }

  if (!fs.existsSync(caFile)) {
    throw new Error(`No existe COBERTURA_CA_FILE: ${caFile}`);
  }

  const ca = fs.readFileSync(caFile, "utf8");
  cachedAgent = new https.Agent({ rejectUnauthorized: true, ca });
  return cachedAgent;
}

function getConfiguredTimezone() {
  loadEnvFile();
  const value = String(process.env.COBERTURA_TIMEZONE || "").trim();
  return value || DEFAULT_TIMEZONE;
}

function getDateTimeParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(date);
  const map = Object.fromEntries(parts.map((item) => [item.type, item.value]));
  return {
    year: map.year,
    month: map.month,
    day: map.day,
    hour: map.hour,
    minute: map.minute,
    second: map.second,
  };
}

function formatDateTimeInTimezone(date = new Date(), options = {}) {
  const timeZone = options.timeZone || getConfiguredTimezone();
  const includeSeconds = options.includeSeconds !== false;
  const parts = getDateTimeParts(date, timeZone);
  const hhmm = `${parts.hour}:${parts.minute}`;
  const timePart = includeSeconds ? `${hhmm}:${parts.second}` : hhmm;
  return `${parts.year}-${parts.month}-${parts.day} ${timePart}`;
}

function formatTimestampSlugInTimezone(date = new Date(), options = {}) {
  return formatDateTimeInTimezone(date, {
    timeZone: options.timeZone,
    includeSeconds: true,
  })
    .replace(" ", "T")
    .replace(/:/g, "-");
}

module.exports = {
  loadEnvFile,
  getHttpsAgent,
  getConfiguredTimezone,
  formatDateTimeInTimezone,
  formatTimestampSlugInTimezone,
};
