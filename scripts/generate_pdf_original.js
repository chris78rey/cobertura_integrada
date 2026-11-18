#!/usr/bin/env node
// Descarga el PDF ORIGINAL de cobertura que genera el portal (https://coberturasalud.msp.gob.ec/),
// en vez de re-dibujarlo (query_live.js + generate_pdf.js). Basado en la versión Zig (CDP):
// llena cédula/fecha, pulsa "Consultar" y guarda los bytes del blob PDF de la página SIN modificarlos.
//
// Dos modos de uso:
//
// 1) Una consulta (mismo contrato de CLI que generate_pdf.js; abre y cierra el navegador):
//      node scripts/generate_pdf_original.js --cedula 1712345678 --fecha 2026-11-17 \
//           --output_name cobertura_xxx --output_dir /ruta/salida [--visible 1]
//    Salida: 0 = PDF guardado | 1 = fallo técnico | 2 = mensaje del portal (sin PDF)
//            3 = límite de consultas del portal (JSON con "retry_after_seconds")
//
// 2) Servidor persistente (--server): UN navegador reutilizado para muchas consultas.
//    Lee JSON por línea en stdin y responde JSON por línea en stdout:
//      -> {"id":1,"cedula":"...","fecha":"2026-11-17","output_name":"x","output_dir":"/ruta"}
//      <- {"id":1,"exit_code":0,"pdfPath":"...","bytes":35214}
//      <- {"id":1,"exit_code":2,"error":"mensaje del portal"}
//    Termina al cerrarse stdin.

const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { loadEnvFile } = require("./runtime_config");

loadEnvFile();

const URL_PORTAL = "https://coberturasalud.msp.gob.ec/";
const REFRESCAR_CADA = 150; // recarga la página cada N consultas para no acumular memoria

function loadPlaywright() {
  const candidates = [null];
  const nodeDir = String(process.env.COBERTURA_NODE_PROJECT_DIR || "").trim();
  if (nodeDir) candidates.push(path.join(nodeDir, "node_modules", "playwright"));
  candidates.push(path.resolve(__dirname, "..", "node_modules", "playwright"));
  let lastError;
  for (const c of candidates) {
    try {
      return require(c || "playwright");
    } catch (error) {
      lastError = error;
    }
  }
  throw new Error(`No se pudo cargar playwright: ${lastError && lastError.message}`);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key.startsWith("--")) continue;
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      args[key.slice(2)] = "1";
    } else {
      args[key.slice(2)] = next;
      i += 1;
    }
  }
  return args;
}

function safeFilename(value) {
  return String(value).replace(/[^a-zA-Z0-9_-]/g, "_");
}

function cedulaValida(c) {
  if (!/^\d{10}$/.test(c)) return false;
  const prov = Number(c.slice(0, 2));
  if (prov < 1 || (prov > 24 && prov !== 30)) return false;
  if (Number(c[2]) > 5) return false;
  let sum = 0;
  for (let i = 0; i < 9; i += 1) {
    let v = Number(c[i]) * (i % 2 === 0 ? 2 : 1);
    if (v > 9) v -= 9;
    sum += v;
  }
  return (10 - (sum % 10)) % 10 === Number(c[9]);
}

// YYYY-MM-DD (o DD-MM-YYYY / DD/MM/YYYY) -> DD-MM-YYYY, que es lo que espera el portal.
function toPortalDate(fecha) {
  const parts = String(fecha).trim().split(/[-/.]/).map((s) => s.trim());
  if (parts.length < 3) return null;
  let [a, b, c] = parts;
  c = c.slice(0, 4); // ignora hora si viene "2026-11-17 00:00"
  if (!/^\d+$/.test(a) || !/^\d+$/.test(b) || !/^\d+$/.test(c)) return null;
  if (a.length === 4) return `${c.padStart(2, "0")}-${b.padStart(2, "0")}-${a}`;
  if (c.length === 4) return `${a.padStart(2, "0")}-${b.padStart(2, "0")}-${c}`;
  return null;
}

// Un PDF completo: empieza con %PDF-, termina con %%EOF y no está vacío/truncado.
function pdfCompleto(buf) {
  if (!buf || buf.length < 1000) return false;
  if (buf.subarray(0, 5).toString("latin1") !== "%PDF-") return false;
  return buf.subarray(Math.max(0, buf.length - 1024)).toString("latin1").includes("%%EOF");
}

class PortalError extends Error {
  constructor(message, code, extra = {}) {
    super(message);
    this.exitCode = code;
    this.extra = extra;
  }
}

// Navegador reutilizable: una pestaña que se limpia con "Nueva consulta" entre consultas.
class Descargador {
  constructor({ visible = false, timeoutMs = 90000, resultMs = 30000 } = {}) {
    this.visible = visible;
    this.resultMs = resultMs;
    this.timeoutMs = timeoutMs;
    this.browser = null;
    this.page = null;
    this.consultas = 0;
  }

  async _lanzar() {
    const { chromium } = loadPlaywright();
    const opts = { headless: !this.visible };
    const exe = String(process.env.CHROME_PATH || "").trim();
    if (exe) opts.executablePath = exe;
    this.browser = await chromium.launch(opts);
    const context = await this.browser.newContext({ acceptDownloads: false });
    this.page = await context.newPage();
    this.page.setDefaultTimeout(this.timeoutMs);
  }

  async _cargarPortal() {
    await this.page.goto(URL_PORTAL, { waitUntil: "domcontentloaded" });
    await this.page.locator("#cedula").waitFor({ state: "visible" });
    this.consultas = 0;
  }

  async iniciar() {
    await this.cerrar();
    await this._lanzar();
    await this._cargarPortal();
  }

  async cerrar() {
    const b = this.browser;
    this.browser = null;
    this.page = null;
    if (b) await b.close().catch(() => {});
  }

  // Tras un fallo técnico: recargar la página; si el navegador murió, relanzarlo.
  async _recuperar() {
    try {
      if (!this.browser || !this.browser.isConnected() || !this.page || this.page.isClosed()) {
        await this.iniciar();
      } else {
        await this._cargarPortal();
      }
    } catch (_) {
      this.browser = null; // la próxima consulta relanza todo
      this.page = null;
    }
  }

  async _escribir(locator, texto) {
    // Igual que la versión Zig: foco, seleccionar todo, Backspace e insertText (compatible con inputs de React).
    await locator.focus();
    await locator.press("Control+A");
    await locator.press("Backspace");
    await this.page.keyboard.insertText(texto);
  }

  async _cerrarDialogo() {
    const dialog = this.page.locator("[role=dialog]");
    if ((await dialog.count()) > 0) {
      await dialog.first().locator("button").first().click({ timeout: 3000 }).catch(() => {});
      await dialog.first().waitFor({ state: "detached", timeout: 3000 }).catch(() => {});
    }
  }

  async _consultar(cedula, fechaPortal) {
    const page = this.page;
    const inputCedula = page.locator("#cedula");

    // Dejar el formulario limpio: cerrar diálogo previo y pulsar "Nueva consulta" si hay resultado en pantalla.
    await this._cerrarDialogo();
    const btnNueva = page.getByRole("button", { name: /^\s*nueva consulta\s*$/i });
    if ((await btnNueva.count()) > 0) {
      await btnNueva.first().click();
      await page.waitForFunction(
        () => !document.querySelector('embed[src^="blob:"]') && !document.querySelector("[role=dialog]"),
        null,
        { timeout: 15000 },
      );
    }
    await inputCedula.waitFor({ state: "visible", timeout: 15000 });

    // Campo fecha: por etiqueta "Fecha de consulta", o el 2.º input como respaldo.
    let inputFecha = page.getByLabel(/fecha de consulta/i);
    if ((await inputFecha.count()) === 0) inputFecha = page.locator("input").nth(1);
    inputFecha = inputFecha.first();

    await this._escribir(inputCedula, cedula);
    await this._escribir(inputFecha, fechaPortal);

    await page.waitForFunction(
      () => {
        const b = [...document.querySelectorAll("button")].find((x) => x.innerText.trim().toLowerCase() === "consultar");
        return !!b && !b.disabled;
      },
      null,
      { timeout: 15000 },
    );
    const btnConsultar = page.getByRole("button", { name: /^\s*consultar\s*$/i }).first();
    await btnConsultar.click();

    // Resultado: o aparece el PDF (embed blob:) o un diálogo con el mensaje del portal.
    // El portal normalmente responde en 1-2 s, pero a veces ignora el clic: si no hay respuesta
    // en unos segundos se vuelve a pulsar "Consultar" una vez antes de esperar el plazo completo.
    const hayResultado = () =>
      !!document.querySelector('embed[src^="blob:"]') || !!document.querySelector("[role=dialog]");
    try {
      await page.waitForFunction(hayResultado, null, { timeout: Math.min(this.resultMs, 10000) });
    } catch (_) {
      // Si el botón sigue deshabilitado, el portal está procesando: solo se espera (no se vuelve a pulsar).
      const habilitado = await page.evaluate(() => {
        const b = [...document.querySelectorAll("button")].find((x) => x.innerText.trim().toLowerCase() === "consultar");
        return !!b && !b.disabled;
      });
      if (habilitado) await btnConsultar.click({ timeout: 3000 }).catch(() => {});
      await page.waitForFunction(hayResultado, null, { timeout: this.resultMs });
    }

    const dialog = page.locator("[role=dialog]");
    if ((await dialog.count()) > 0) {
      const raw = await dialog.first().innerText();
      let msg = raw.replace(/\s+/g, " ").trim();
      msg = msg.replace(/^Mensaje\s+/i, "").replace(/\s+ok$/i, "").trim();
      await this._cerrarDialogo();
      if (/mite de consultas/i.test(msg)) {
        throw new PortalError(msg, 3, { retry_after_seconds: /1 hora/i.test(msg) ? 61 * 60 : 135 });
      }
      throw new PortalError(msg || "El portal devolvió un mensaje vacío", 2);
    }

    const b64 = await page.evaluate(async () => {
      const buf = new Uint8Array(await (await fetch(document.querySelector("embed").src)).arrayBuffer());
      let s = "";
      for (let i = 0; i < buf.length; i += 0x8000) s += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
      return btoa(s);
    });
    const pdf = Buffer.from(b64, "base64");
    if (pdf.subarray(0, 5).toString("latin1") !== "%PDF-") {
      throw new PortalError("El blob descargado no es un PDF", 2);
    }
    return pdf;
  }

  async descargar(cedula, fechaPortal) {
    try {
      if (!this.browser || !this.page || this.page.isClosed()) await this.iniciar();
      if (this.consultas >= REFRESCAR_CADA) await this._cargarPortal();
      this.consultas += 1;
      return await this._consultar(cedula, fechaPortal);
    } catch (error) {
      if (!(error instanceof PortalError)) await this._recuperar();
      throw error;
    }
  }
}

function validarEntrada(cedula, fecha) {
  const ced = String(cedula || "").trim();
  if (!ced || !fecha) throw new Error("Usa --cedula y --fecha.");
  if (!cedulaValida(ced)) throw new Error(`Cédula con formato/dígito verificador inválido: ${ced}`);
  const fechaPortal = toPortalDate(fecha);
  if (!fechaPortal) throw new Error(`Fecha no reconocida: ${fecha}`);
  return { cedula: ced, fechaPortal };
}

function guardarPdf(pdf, { cedula, fecha, outputName, outputDir }) {
  if (!pdfCompleto(pdf)) throw new PortalError("El PDF descargado está incompleto (sin %%EOF o muy pequeño)", 1);
  const dir = path.resolve(outputDir || "output");
  fs.mkdirSync(dir, { recursive: true });
  const base = outputName ? safeFilename(outputName) : `cobertura_${safeFilename(cedula)}_${safeFilename(fecha)}`;
  const pdfPath = path.join(dir, `${base}.pdf`);
  const tmp = `${pdfPath}.part`;
  fs.writeFileSync(tmp, pdf);
  fs.renameSync(tmp, pdfPath); // escritura atómica: nunca queda un PDF a medias
  // Este modo no genera JSON; borrar un sidecar viejo evita falsos "servicio no disponible" en Python.
  try {
    fs.unlinkSync(path.join(dir, `${base}.json`));
  } catch (_) {
    // no existía
  }
  return pdfPath;
}

async function modoServidor(args) {
  const timeoutMs = Math.max(10, Number(args.timeout || 90)) * 1000;
  const d = new Descargador({ visible: args.visible === "1", timeoutMs });
  const responder = (obj) => process.stdout.write(`${JSON.stringify(obj)}\n`);
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  responder({ ready: true });
  for await (const line of rl) {
    if (!line.trim()) continue;
    let req;
    try {
      req = JSON.parse(line);
    } catch (_) {
      continue;
    }
    try {
      const { cedula, fechaPortal } = validarEntrada(req.cedula, req.fecha);
      const pdf = await d.descargar(cedula, fechaPortal);
      const pdfPath = guardarPdf(pdf, {
        cedula,
        fecha: req.fecha,
        outputName: req.output_name,
        outputDir: req.output_dir,
      });
      responder({ id: req.id, exit_code: 0, pdfPath, bytes: pdf.length, source: "portal_original" });
    } catch (error) {
      responder({ id: req.id, exit_code: error.exitCode || 1, error: error.message, ...(error.extra || {}) });
    }
  }
  await d.cerrar();
}

async function modoUnico(args) {
  const { cedula, fechaPortal } = validarEntrada(args.cedula, args.fecha);
  const timeoutMs = Math.max(10, Number(args.timeout || 90)) * 1000;
  const d = new Descargador({ visible: args.visible === "1", timeoutMs });
  try {
    const pdf = await d.descargar(cedula, fechaPortal);
    const pdfPath = guardarPdf(pdf, {
      cedula,
      fecha: args.fecha,
      outputName: args.output_name,
      outputDir: args.output_dir || args.outputDir || args["output-dir"],
    });
    console.log(JSON.stringify({ pdfPath, bytes: pdf.length, source: "portal_original" }, null, 2));
  } finally {
    await d.cerrar();
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.server === "1") return modoServidor(args);
  return modoUnico(args);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    console.log(JSON.stringify({ error: error.message, ...(error.extra || {}) }));
    process.exit(error.exitCode || 1);
  });
}

module.exports = { Descargador, toPortalDate, cedulaValida, pdfCompleto };
