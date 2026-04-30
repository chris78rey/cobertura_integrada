const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const https = require("https");
const CryptoJS = require("crypto-js");
const { getHttpsAgent, loadEnvFile } = require("./runtime_config");

const BASE_URL = "https://coberturasalud.msp.gob.ec/";
const ACTION_GET_CAPTCHA = "40e5613a02e25c0dfb759fd7f199149081432edf13";
const ACTION_API_CLIENT = "70987a4dcfb783907102d476e4a450486019bbcc62";

loadEnvFile();

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (key.startsWith("--")) {
      args[key.slice(2)] = value;
      i += 1;
    }
  }
  return args;
}

function validateCedula(cedula) {
  if (!/^\d{10}$/.test(cedula || "")) {
    throw new Error("La cédula debe tener 10 dígitos.");
  }
}

function validateFecha(fecha) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(fecha || "")) {
    throw new Error("La fecha debe usar formato YYYY-MM-DD.");
  }
}

function toPortalDate(fechaIso) {
  const [year, month, day] = fechaIso.split("-");
  return `${day}-${month}-${year}`;
}

function csconsulta(cedula) {
  let out = "";
  for (let index = 0; index < cedula.length; index += 2) {
    const a = Number(cedula[index]);
    const b = cedula[index + 1] ? Number(cedula[index + 1]) : 0;
    out += String(a + b);
  }
  return out;
}

function parseRscPayload(text) {
  const line = text
    .split(/\r?\n/)
    .find((candidate) => candidate.startsWith("1:"));
  if (!line) {
    throw new Error(`Respuesta RSC no reconocida: ${text.slice(0, 500)}`);
  }
  return JSON.parse(line.slice(2));
}

function extractCookie(headers) {
  const setCookieHeader = headers["set-cookie"];
  const setCookie = Array.isArray(setCookieHeader) ? setCookieHeader[0] : setCookieHeader;
  if (!setCookie) {
    return "";
  }
  return setCookie.split(";")[0];
}

const BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function postText(url, headers, body) {
  return new Promise((resolve, reject) => {
    const payload = String(body);
    const request = https.request(
      url,
      {
        method: "POST",
        headers: {
          ...headers,
          "user-agent": BROWSER_UA,
          "content-length": Buffer.byteLength(payload),
        },
        agent: getHttpsAgent(),
      },
      (response) => {
        let chunks = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          chunks += chunk;
        });
        response.on("end", () => {
          resolve({
            status: response.statusCode || 0,
            headers: response.headers,
            body: chunks,
          });
        });
      }
    );

    request.on("error", (error) => {
      const tlsHelp =
        "Si es un problema de certificados en Ubuntu, usa COBERTURA_TLS_INSECURE=true temporalmente o configura COBERTURA_CA_FILE.";
      reject(new Error(`${error.message}. ${tlsHelp}`));
    });

    request.write(payload);
    request.end();
  });
}

async function postAction(actionId, body, cookie = "") {
  const response = await postText(
    BASE_URL,
    {
      "content-type": "text/plain;charset=UTF-8",
      "next-action": actionId,
      ...(cookie ? { cookie } : {}),
    },
    JSON.stringify(body)
  );

  const text = response.body;
  return {
    status: response.status,
    cookie: extractCookie(response.headers) || cookie,
    payload: parseRscPayload(text),
    raw: text,
  };
}

async function runSingle(cedula, fecha) {
  validateCedula(cedula);
  validateFecha(fecha);

  const reqId = crypto.randomUUID();
  const captcha = await postAction(ACTION_GET_CAPTCHA, [reqId]);
  if (captcha.payload.success !== "success") {
    throw new Error(`getCaptcha falló: ${captcha.raw.slice(0, 500)}`);
  }

  const { token, reqId: echoedReqId } = captcha.payload.data;
  if (echoedReqId !== reqId) {
    throw new Error("reqId no coincide en getCaptcha.");
  }

  const consultaPayload = [
    "cobertura",
    "POST",
    {
      identificacion: CryptoJS.AES.encrypt(cedula, token).toString(),
      fechaConsulta: CryptoJS.AES.encrypt(toPortalDate(fecha), token).toString(),
      token,
      csconsulta: csconsulta(cedula),
    },
  ];

  const result = await postAction(ACTION_API_CLIENT, consultaPayload, captcha.cookie);
  return {
    request: {
      cedula,
      fecha,
      reqId,
      token,
      csconsulta: csconsulta(cedula),
    },
    response: result.payload,
  };
}

function loadCsv(csvPath) {
  const rows = fs.readFileSync(csvPath, "utf8").trim().split(/\r?\n/);
  const [header, ...data] = rows;
  const columns = header.split(",").map((item) => item.trim());
  const cedulaIndex = columns.indexOf("cedula");
  const fechaIndex = columns.indexOf("fecha");
  if (cedulaIndex < 0 || fechaIndex < 0) {
    throw new Error("El CSV debe tener columnas cedula,fecha");
  }
  return data
    .map((line) => line.split(","))
    .filter((parts) => parts.length >= 2)
    .map((parts) => ({
      cedula: parts[cedulaIndex].trim(),
      fecha: parts[fechaIndex].trim(),
    }));
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.input) {
    const items = loadCsv(path.resolve(args.input));
    const results = [];
    for (const item of items) {
      try {
        results.push(await runSingle(item.cedula, item.fecha));
      } catch (error) {
        results.push({
          request: item,
          error: error.message,
        });
      }
    }
    console.log(JSON.stringify(results, null, 2));
    return;
  }

  if (!args.cedula || !args.fecha) {
    throw new Error("Usa --cedula y --fecha, o --input.");
  }

  const result = await runSingle(args.cedula, args.fecha);
  console.log(JSON.stringify(result, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  runSingle,
  toPortalDate,
  csconsulta,
};
