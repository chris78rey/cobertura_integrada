const fs = require("fs");
const path = require("path");
const PDFDocument = require("pdfkit");
const SVGtoPDF = require("svg-to-pdfkit");
const { runSingle } = require("./query_live");
const { formatDateTimeInTimezone } = require("./runtime_config");
const { loadSvgLayout } = require("./svg_layout");

const PAGE_WIDTH = 841.89;
const PAGE_HEIGHT = 595.28;
const REPORT_FORCE_BOLD = true;

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

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function safeFilename(value) {
  return String(value).replace(/[^a-zA-Z0-9_-]/g, "_");
}

function formatCoverageDate(fechaIso) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(fechaIso || ""))) {
    return fechaIso || "";
  }
  const date = new Date(`${fechaIso}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return fechaIso;
  }
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    day: "2-digit",
    month: "long",
    year: "numeric",
  }).format(date);
}

function resolveBaseName(cedula, fecha, outputName = "") {
  if (outputName) {
    return safeFilename(outputName);
  }
  return `cobertura_${safeFilename(cedula)}_${safeFilename(fecha)}`;
}

function parsePrivados(data) {
  const source =
    data && data.coberturaPrivada && data.coberturaPrivada.RegistrosAsegurados
      ? data.coberturaPrivada.RegistrosAsegurados.RegistroAsegurado
      : [];

  if (!source) {
    return [];
  }
  if (Array.isArray(source)) {
    return source;
  }
  return [source];
}

function escapeXml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function svgAssetHref(outDir, fileName) {
  const absoluteAssetPath = path.resolve("assets", fileName);
  if (!fs.existsSync(absoluteAssetPath)) {
    return "";
  }
  return path.relative(outDir, absoluteAssetPath).replace(/\\/g, "/");
}

function wrapText(text, width, fontSize) {
  const raw = String(text || "-");
  const parts = raw.split(/\n/);
  const maxChars = Math.max(8, Math.floor(width / Math.max(1, fontSize * 0.55)));
  const lines = [];
  for (const part of parts) {
    const words = part.split(/\s+/).filter(Boolean);
    if (!words.length) {
      lines.push("");
      continue;
    }
    let current = words[0];
    for (let index = 1; index < words.length; index += 1) {
      const candidate = `${current} ${words[index]}`;
      if (candidate.length <= maxChars) {
        current = candidate;
      } else {
        lines.push(current);
        current = words[index];
      }
    }
    lines.push(current);
  }
  return lines;
}

function svgText({
  text,
  x,
  y,
  size = 10,
  bold = false,
  anchor = "start",
  width = 0,
  lineHeight = 1.2,
  fill = "#000000",
}) {
  const effectiveBold = bold || REPORT_FORCE_BOLD;
  const lines = width > 0 ? wrapText(text, width, size) : String(text || "").split(/\n/);
  const tspans = lines
    .map((line, index) => {
      const dy = index === 0 ? 0 : size * lineHeight;
      return `<tspan x="${x}" dy="${dy}">${escapeXml(line)}</tspan>`;
    })
    .join("");
  return `<text x="${x}" y="${y}" font-family="Helvetica" font-size="${size}" font-weight="${
    effectiveBold ? "700" : "400"
  }" text-anchor="${anchor}" fill="${fill}">${tspans}</text>`;
}

function svgTable({ x, y, widths, headers, rows, headerHeight, rowHeight, headerFontSize, bodyFontSize, lineWidth }) {
  const elements = [];
  let cursorX = x;
  for (let index = 0; index < headers.length; index += 1) {
    const width = widths[index];
    elements.push(`<rect x="${cursorX}" y="${y}" width="${width}" height="${headerHeight}" fill="none" stroke="#000" stroke-width="${lineWidth}"/>`);
    elements.push(
      svgText({
        text: headers[index],
        x: cursorX + width / 2,
        y: y + headerFontSize + 5,
        size: headerFontSize,
        bold: true,
        anchor: "middle",
        width: width - 12,
      })
    );
    cursorX += width;
  }

  let rowY = y + headerHeight;
  for (const row of rows) {
    cursorX = x;
    for (let index = 0; index < widths.length; index += 1) {
      const width = widths[index];
      elements.push(
        `<rect x="${cursorX}" y="${rowY}" width="${width}" height="${rowHeight}" fill="none" stroke="#000" stroke-width="${lineWidth}"/>`
      );
      elements.push(
        svgText({
          text: row[index] || "-",
          x: cursorX + 5,
          y: rowY + bodyFontSize + 5,
          size: bodyFontSize,
          width: width - 10,
        })
      );
      cursorX += width;
    }
    rowY += rowHeight;
  }
  return elements.join("\n");
}

function estimateRowHeight(row, widths, fontSize, minHeight, options = {}) {
  const lineHeight = options.lineHeight || 1.2;
  const cellPaddingY = options.cellPaddingY || 5;
  let maxLines = 1;
  for (let index = 0; index < widths.length; index += 1) {
    const cell = row[index] || "-";
    const lines = wrapText(cell, Math.max(20, widths[index] - 10), fontSize);
    maxLines = Math.max(maxLines, lines.length);
  }
  const dynamicHeight = maxLines * fontSize * lineHeight + cellPaddingY * 2;
  return Math.max(minHeight, Math.ceil(dynamicHeight));
}

function svgTableDynamic({
  x,
  y,
  widths,
  headers,
  rows,
  rowHeights,
  headerHeight,
  headerFontSize,
  bodyFontSize,
  lineWidth,
}) {
  const elements = [];
  let cursorX = x;
  for (let index = 0; index < headers.length; index += 1) {
    const width = widths[index];
    elements.push(`<rect x="${cursorX}" y="${y}" width="${width}" height="${headerHeight}" fill="none" stroke="#000" stroke-width="${lineWidth}"/>`);
    elements.push(
      svgText({
        text: headers[index],
        x: cursorX + width / 2,
        y: y + headerFontSize + 5,
        size: headerFontSize,
        bold: true,
        anchor: "middle",
        width: width - 12,
      })
    );
    cursorX += width;
  }

  let rowY = y + headerHeight;
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const rowHeight = rowHeights[rowIndex];
    cursorX = x;
    for (let colIndex = 0; colIndex < widths.length; colIndex += 1) {
      const width = widths[colIndex];
      elements.push(
        `<rect x="${cursorX}" y="${rowY}" width="${width}" height="${rowHeight}" fill="none" stroke="#000" stroke-width="${lineWidth}"/>`
      );
      elements.push(
        svgText({
          text: row[colIndex] || "-",
          x: cursorX + 5,
          y: rowY + bodyFontSize + 5,
          size: bodyFontSize,
          width: width - 10,
        })
      );
      cursorX += width;
    }
    rowY += rowHeight;
  }
  return elements.join("\n");
}

function paginateRows(rows, rowHeights, maxContentHeight, headerHeight) {
  const pages = [];
  let cursor = 0;
  while (cursor < rows.length) {
    let used = headerHeight;
    const pageRows = [];
    const pageHeights = [];
    while (cursor < rows.length) {
      const nextHeight = rowHeights[cursor];
      if (pageRows.length > 0 && used + nextHeight > maxContentHeight) {
        break;
      }
      pageRows.push(rows[cursor]);
      pageHeights.push(nextHeight);
      used += nextHeight;
      cursor += 1;
      if (pageRows.length === 1 && used > maxContentHeight) {
        break;
      }
    }
    pages.push({ rows: pageRows, rowHeights: pageHeights });
  }
  if (!pages.length) {
    pages.push({ rows: [], rowHeights: [] });
  }
  return pages;
}

function buildFooterImages(outDir) {
  const footerLogos = ["logomsp.jpg", "mininterior.jpg", "mindefensa.jpg", "iess.jpg", "issfa.jpg", "isspol.jpg"];
  const footerSizes = [
    [76, 22],
    [76, 22],
    [76, 22],
    [46, 22],
    [46, 22],
    [46, 22],
  ];

  let footerX = PAGE_WIDTH / 2 - 205;
  const footerImages = [];
  for (let index = 0; index < footerLogos.length; index += 1) {
    const href = svgAssetHref(outDir, footerLogos[index]);
    const [width, height] = footerSizes[index];
    if (href) {
      footerImages.push(`<image href="${escapeXml(href)}" x="${footerX}" y="${PAGE_HEIGHT - 99}" width="${width}" height="${height}"/>`);
    } else {
      footerImages.push(`<rect x="${footerX}" y="${PAGE_HEIGHT - 99}" width="${width}" height="${height}" fill="none" stroke="#000" stroke-width="0.8"/>`);
    }
    footerX += width + 16;
  }
  return footerImages;
}

function buildSvgPage({
  body,
  pageNumber,
  totalPages,
  layout,
  outDir,
}) {
  const mspHref = svgAssetHref(outDir, "logomsp.jpg");
  const rpisHref = svgAssetHref(outDir, "logorpis.jpg");
  const escudoHref =
    svgAssetHref(outDir, "escudo_ec.png") || svgAssetHref(outDir, "escudo_ec.jpg") || svgAssetHref(outDir, "escudo_ec.jpeg");
  const footerImages = buildFooterImages(outDir);

  const escudoScale = 0.84;
  const escudoWidth = layout.escudo.width * escudoScale;
  const escudoHeight = layout.escudo.height * escudoScale;
  const escudoX = layout.escudo.x + (layout.escudo.width - escudoWidth) / 2;
  const escudoY = layout.escudo.y + (layout.escudo.height - escudoHeight) / 2;

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${PAGE_WIDTH}" height="${PAGE_HEIGHT}" viewBox="0 0 ${PAGE_WIDTH} ${PAGE_HEIGHT}">
  <line x1="20" y1="${layout.headerLineY}" x2="${PAGE_WIDTH - 20}" y2="${layout.headerLineY}" stroke="#000" stroke-width="1.1"/>
  ${mspHref ? `<image href="${escapeXml(mspHref)}" x="${layout.mspLogo.x}" y="${layout.mspLogo.y}" width="${layout.mspLogo.width}" height="${layout.mspLogo.height}"/>` : ""}
  ${rpisHref ? `<image href="${escapeXml(rpisHref)}" x="${layout.rpisLogo.x}" y="${layout.rpisLogo.y}" width="${layout.rpisLogo.width}" height="${layout.rpisLogo.height}"/>` : ""}
  ${escudoHref ? `<image href="${escapeXml(escudoHref)}" x="${escudoX}" y="${escudoY}" width="${escudoWidth}" height="${escudoHeight}" opacity="0.62"/>` : `<rect x="${layout.escudo.x}" y="${layout.escudo.y}" width="${layout.escudo.width}" height="${layout.escudo.height}" fill="none" stroke="#000" stroke-width="1"/>${svgText({ text: "EC", x: layout.escudo.x + layout.escudo.width / 2, y: layout.escudo.y + layout.escudo.height / 2 + 5, size: 13, bold: true, anchor: "middle" })}`}

  ${body}

  <line x1="20" y1="${PAGE_HEIGHT - 108}" x2="${PAGE_WIDTH - 20}" y2="${PAGE_HEIGHT - 108}" stroke="#000" stroke-width="1.1"/>
  ${footerImages.join("\n")}
  <line x1="20" y1="${PAGE_HEIGHT - 76}" x2="${PAGE_WIDTH - 20}" y2="${PAGE_HEIGHT - 76}" stroke="#000" stroke-width="1.1"/>
  ${svgText({ text: `${pageNumber} / ${totalPages}`, x: PAGE_WIDTH / 2, y: PAGE_HEIGHT - 61, size: 7.2, anchor: "middle" })}
  ${svgText({ text: "Plataforma Gubernamental de Desarrollo Social", x: PAGE_WIDTH - 34, y: PAGE_HEIGHT - 69, size: 6.8, anchor: "end" })}
  ${svgText({ text: "Av. Quitumbe Nan y Amaru Nan", x: PAGE_WIDTH - 34, y: PAGE_HEIGHT - 57, size: 6.8, anchor: "end" })}
  ${svgText({ text: "Telf: 593 (2) 3814400  |  www.msp.gob.ec", x: PAGE_WIDTH - 34, y: PAGE_HEIGHT - 45, size: 6.8, anchor: "end" })}
</svg>`;
}

function generateSvgFromResult({ result, cedula, fecha, outputName = "", outputDir = "output" }) {
  const data = result.response.data;
  const seguros =
    data && data.coberturaSalud && data.coberturaSalud.CoberturaSeguros
      ? data.coberturaSalud.CoberturaSeguros.aseguradora || []
      : [];
  const privados = parsePrivados(data);

  const outDir = path.resolve(outputDir);
  ensureDir(outDir);
  const baseName = resolveBaseName(cedula, fecha, outputName);
  const layout = loadSvgLayout();

  const nombre = seguros.find((item) => item.Nombre)?.Nombre || "";

  const mspHref = svgAssetHref(outDir, "logomsp.jpg");
  const rpisHref = svgAssetHref(outDir, "logorpis.jpg");
  const escudoHref =
    svgAssetHref(outDir, "escudo_ec.png") || svgAssetHref(outDir, "escudo_ec.jpg") || svgAssetHref(outDir, "escudo_ec.jpeg");

  const privadosConFinanciador = privados.filter((item) => String(item && item.NombreFinanciador ? item.NombreFinanciador : "").trim());
  const showPrivateTable = privadosConFinanciador.length > 0;
  const privateRows = showPrivateTable
    ? privadosConFinanciador.map((item) => [
        item.RucEmpresa || "",
        item.NombreFinanciador || "",
        item.IdentificacionBeneficiario || "",
        item.NombreBeneficiario || "",
        item.ApellidosBeneficiario || "",
      ])
    : [];

  const segurosRows = (seguros.length ? seguros : [{
    NombreInstitucion: "-",
    TipoSeguro: "Servicio no disponible",
    MensajeServicioExterno: "Servicio no disponible",
    EstadoCobertura: "Servicio no disponible",
  }]).map((item) => [
    item.NombreInstitucion || "",
    item.TipoSeguro || "Servicio no disponible",
    item.MensajeServicioExterno || "Servicio no disponible",
    item.EstadoCobertura || "Servicio no disponible",
  ]);

  const segurosWidths = [92, 188, 255, 195];
  const segurosHeaderHeight = 26;
  const segurosBodyFont = 7.1;
  const segurosLineWidth = 0.55;
  const segurosRowHeights = segurosRows.map((row) => estimateRowHeight(row, segurosWidths, segurosBodyFont, 31));

  const privadosWidths = [104, 248, 150, 105, 108];
  const privadosHeaderHeight = 20;
  const privadosBodyFont = 6.9;
  const privadosLineWidth = 0.55;
  const privadosRowHeights = showPrivateTable
    ? privateRows.map((row) => estimateRowHeight(row, privadosWidths, privadosBodyFont, 19))
    : [];

  const mainTableTop = 202;
  const mainTableBottom =
    mainTableTop + segurosHeaderHeight + segurosRowHeights.reduce((sum, value) => sum + value, 0);
  const noteY = mainTableBottom + 16;
  const privateTitleY = noteY + 18;
  const privateTableY = privateTitleY + 14;
  const fechaConsultaY = PAGE_HEIGHT - 141;
  const privateBottomLimit = fechaConsultaY - 8;
  const privateAvailableHeight = Math.max(40, privateBottomLimit - privateTableY);

  const privatePages = showPrivateTable
    ? paginateRows(privateRows, privadosRowHeights, privateAvailableHeight, privadosHeaderHeight)
    : [];

  const pageBodies = [];
  const firstPrivatePage = showPrivateTable ? privatePages.shift() || { rows: [], rowHeights: [] } : { rows: [], rowHeights: [] };
  pageBodies.push(`
  ${svgText({ text: "RED PUBLICA INTEGRAL DE SALUD", x: PAGE_WIDTH / 2, y: 100, size: 13.5, bold: true, anchor: "middle" })}
  ${svgText({ text: "CONSULTA DE COBERTURA DE SALUD", x: PAGE_WIDTH / 2, y: 128, size: 10.5, bold: true, anchor: "middle" })}
  ${svgText({ text: nombre, x: 50, y: 152, size: 7.8, bold: true })}
  ${svgText({ text: "Numero de documento de Identificacion:", x: 50, y: 174, size: 8.2, bold: true })}
  ${svgText({ text: cedula, x: 272, y: 174, size: 8.2 })}
  ${svgText({ text: "Fecha de Cobertura de Seguro de Salud:", x: 420, y: 174, size: 8.2, bold: true })}
  ${svgText({ text: formatCoverageDate(fecha), x: PAGE_WIDTH - 50, y: 174, size: 8.2, anchor: "end" })}
  ${svgText({ text: "IESS, ISSFA, ISSPOL", x: PAGE_WIDTH / 2, y: 194, size: 8.3, bold: true, anchor: "middle" })}

  ${svgTableDynamic({
    x: 50,
    y: mainTableTop,
    widths: segurosWidths,
    headers: ["Seguro", "Tipo de seguro", "Mensaje", "Registro de Cobertura\nde Atencion de Salud"],
    rows: segurosRows,
    rowHeights: segurosRowHeights,
    headerHeight: segurosHeaderHeight,
    headerFontSize: 7.6,
    bodyFontSize: segurosBodyFont,
    lineWidth: segurosLineWidth,
  })}

  ${svgText({ text: "* La informacion historica reflejada corresponde a datos\ndesde Junio 2010", x: 70, y: noteY, size: 6.2, fill: "#0000ff" })}
  ${showPrivateTable ? svgText({ text: "RED PRIVADA COMPLEMENTARIA", x: 70, y: privateTitleY, size: 8, bold: true }) : ""}
  ${showPrivateTable
    ? svgTableDynamic({
        x: 50,
        y: privateTableY,
        widths: privadosWidths,
        headers: ["RUC", "Nombre del Financiador", "Identificacion del\nBeneficiario", "Nombres", "Apellidos"],
        rows: firstPrivatePage.rows,
        rowHeights: firstPrivatePage.rowHeights,
        headerHeight: privadosHeaderHeight,
        headerFontSize: 7.2,
        bodyFontSize: privadosBodyFont,
        lineWidth: privadosLineWidth,
      })
    : ""}
  ${svgText({ text: "Fecha de consulta:", x: 488, y: fechaConsultaY, size: 8.3, bold: true })}
  ${svgText({ text: formatDateTimeInTimezone(new Date(), { includeSeconds: false }), x: 690, y: fechaConsultaY, size: 8.3, anchor: "end" })}
  `);

  for (const privatePage of showPrivateTable ? privatePages : []) {
    pageBodies.push(`
    ${svgText({ text: "RED PRIVADA COMPLEMENTARIA (continuacion)", x: PAGE_WIDTH / 2, y: 120, size: 10, bold: true, anchor: "middle" })}
    ${svgTableDynamic({
      x: 50,
      y: 142,
      widths: privadosWidths,
      headers: ["RUC", "Nombre del Financiador", "Identificacion del\nBeneficiario", "Nombres", "Apellidos"],
      rows: privatePage.rows,
      rowHeights: privatePage.rowHeights,
      headerHeight: privadosHeaderHeight,
      headerFontSize: 7.2,
      bodyFontSize: privadosBodyFont,
      lineWidth: privadosLineWidth,
    })}
    ${svgText({ text: "Fecha de consulta:", x: 488, y: fechaConsultaY, size: 8.3, bold: true })}
    ${svgText({ text: formatDateTimeInTimezone(new Date(), { includeSeconds: false }), x: 690, y: fechaConsultaY, size: 8.3, anchor: "end" })}
    `);
  }

  const totalPages = pageBodies.length;
  const svgPaths = [];
  for (let index = 0; index < pageBodies.length; index += 1) {
    const pageNumber = index + 1;
    const pageSuffix = totalPages > 1 ? `_p${pageNumber}` : "";
    const svgPath = path.join(outDir, `${baseName}${pageSuffix}.svg`);
    const svgContent = buildSvgPage({
      body: pageBodies[index],
      pageNumber,
      totalPages,
      layout,
      outDir,
    });
    fs.writeFileSync(svgPath, svgContent, "utf8");
    svgPaths.push(svgPath);
  }

  return { svgPath: svgPaths[0], svgPaths };
}

function writeLabelValue(doc, label, value, options = {}) {
  const {
    x = 50,
    y = doc.y,
    labelWidth = 220,
    valueWidth = 180,
    gap = 8,
    size = 10,
    align = "left",
  } = options;
  doc.font("Helvetica-Bold").fontSize(size).text(label, x, y, {
    width: labelWidth,
    lineBreak: false,
  });
  doc.font("Helvetica").fontSize(size).text(value || "-", x + labelWidth + gap, y, {
    width: valueWidth,
    align,
    lineBreak: false,
  });
}

function drawTable(doc, headers, rows, widths, options = {}) {
  const startX = options.startX || 50;
  const headerHeight = options.headerHeight || 24;
  const rowHeight = options.rowHeight || 36;
  const headerFontSize = options.headerFontSize || 9;
  const bodyFontSize = options.bodyFontSize || 8.5;
  const headerTopPadding = options.headerTopPadding || 7;
  const cellPaddingX = options.cellPaddingX || 6;
  const cellPaddingY = options.cellPaddingY || 6;
  const lineWidth = options.lineWidth || 0.6;
  let y = options.y || doc.y;

  doc.lineWidth(lineWidth);
  doc.font("Helvetica-Bold").fontSize(headerFontSize);
  let x = startX;
  headers.forEach((header, index) => {
    doc.rect(x, y, widths[index], headerHeight).stroke();
    doc.text(header, x + cellPaddingX, y + headerTopPadding, {
      width: widths[index] - cellPaddingX * 2,
      align: "center",
    });
    x += widths[index];
  });

  y += headerHeight;
  doc.font("Helvetica").fontSize(bodyFontSize);
  rows.forEach((row) => {
    x = startX;
    row.forEach((cell, index) => {
      doc.rect(x, y, widths[index], rowHeight).stroke();
      doc.text(cell || "-", x + cellPaddingX, y + cellPaddingY, {
        width: widths[index] - cellPaddingX * 2,
        height: rowHeight - cellPaddingY * 2,
      });
      x += widths[index];
    });
    y += rowHeight;
  });

  doc.y = y + 12;
  return y;
}

function drawHeader(doc, pageWidth) {
  const previousX = doc.x;
  const previousY = doc.y;
  const layout = loadSvgLayout();
  doc.lineWidth(1.1);
  doc.moveTo(20, layout.headerLineY).lineTo(pageWidth - 20, layout.headerLineY).stroke();
  const mspLogo = path.resolve("assets", "logomsp.jpg");
  const rpisLogo = path.resolve("assets", "logorpis.jpg");
  const escudoCandidates = [
    path.resolve("assets", "escudo_ec.png"),
    path.resolve("assets", "escudo_ec.jpg"),
    path.resolve("assets", "escudo_ec.jpeg"),
  ];
  const escudoAsset = escudoCandidates.find((candidate) => fs.existsSync(candidate));

  if (fs.existsSync(mspLogo)) {
    doc.image(mspLogo, layout.mspLogo.x, layout.mspLogo.y, {
      fit: [layout.mspLogo.width, layout.mspLogo.height],
      align: "left",
      valign: "center",
    });
  } else {
    doc.rect(layout.mspLogo.x, layout.mspLogo.y, layout.mspLogo.width, layout.mspLogo.height).stroke();
    doc.font("Helvetica-Bold").fontSize(10).text("MSP", layout.mspLogo.x + 44, layout.mspLogo.y + 4, {
      align: "center",
      width: 32,
      lineBreak: false,
    });
  }

  if (fs.existsSync(rpisLogo)) {
    doc.image(rpisLogo, layout.rpisLogo.x, layout.rpisLogo.y, {
      fit: [layout.rpisLogo.width, layout.rpisLogo.height],
      align: "right",
      valign: "center",
    });
  }

  if (escudoAsset) {
    const escudoScale = 0.84;
    const escudoWidth = layout.escudo.width * escudoScale;
    const escudoHeight = layout.escudo.height * escudoScale;
    const escudoX = layout.escudo.x + (layout.escudo.width - escudoWidth) / 2;
    const escudoY = layout.escudo.y + (layout.escudo.height - escudoHeight) / 2;
    doc.save();
    doc.opacity(0.62);
    doc.image(escudoAsset, escudoX, escudoY, {
      fit: [escudoWidth, escudoHeight],
      align: "center",
      valign: "center",
    });
    doc.restore();
  } else {
    doc.rect(layout.escudo.x, layout.escudo.y, layout.escudo.width, layout.escudo.height).stroke();
    doc.font("Helvetica-Bold").fontSize(13).text("EC", layout.escudo.x + 8, layout.escudo.y + 12, {
      width: Math.max(1, layout.escudo.width - 16),
      align: "center",
      lineBreak: false,
    });
  }
  doc.x = previousX;
  doc.y = previousY;
}

function drawFooter(doc, pageWidth, pageHeight) {
  const previousX = doc.x;
  const previousY = doc.y;
  const footerInfoX = pageWidth - 212;
  const footerInfoWidth = 178;
  doc.lineWidth(1.1);
  doc.moveTo(20, pageHeight - 108).lineTo(pageWidth - 20, pageHeight - 108).stroke();
  const logoY = pageHeight - 99;
  const logos = [
    ["logomsp.jpg", 76, 22],
    ["mininterior.jpg", 76, 22],
    ["mindefensa.jpg", 76, 22],
    ["iess.jpg", 46, 22],
    ["issfa.jpg", 46, 22],
    ["isspol.jpg", 46, 22],
  ];
  let x = pageWidth / 2 - 205;
  logos.forEach(([fileName, width, height]) => {
    const filePath = path.resolve("assets", fileName);
    if (fs.existsSync(filePath)) {
      doc.image(filePath, x, logoY, { fit: [width, height], align: "center", valign: "center" });
    } else {
      doc.rect(x, logoY, width, height).stroke();
    }
    x += width + 16;
  });

  doc.moveTo(20, pageHeight - 76).lineTo(pageWidth - 20, pageHeight - 76).stroke();
  doc.font("Helvetica").fontSize(6.8);
  doc.text("1 / 1", pageWidth / 2 - 10, pageHeight - 65, { width: 20, align: "center", lineBreak: false });
  doc.text("Plataforma Gubernamental de Desarrollo Social", footerInfoX, pageHeight - 68, {
    width: footerInfoWidth,
    align: "right",
    lineBreak: false,
  });
  doc.text("Av. Quitumbe Nan y Amaru Nan", footerInfoX, pageHeight - 56, {
    width: footerInfoWidth,
    align: "right",
    lineBreak: false,
  });
  doc.text("Telf: 593 (2) 3814400  |  www.msp.gob.ec", footerInfoX, pageHeight - 44, {
    width: footerInfoWidth,
    align: "right",
    lineBreak: false,
  });
  doc.x = previousX;
  doc.y = previousY;
}

function applyChromeToAllPages(doc) {
  const range = doc.bufferedPageRange();
  for (let index = range.start; index < range.start + range.count; index += 1) {
    doc.switchToPage(index);
    const pageWidth = doc.page.width;
    const pageHeight = doc.page.height;
    drawHeader(doc, pageWidth);
    drawFooter(doc, pageWidth, pageHeight);
  }
}

async function renderPdfFromSvg({ svgPath, svgPaths, pdfPath }) {
  const pages = Array.isArray(svgPaths) && svgPaths.length ? svgPaths : [svgPath];
  const doc = new PDFDocument({
    size: "A4",
    layout: "landscape",
    margin: 0,
  });
  const stream = fs.createWriteStream(pdfPath);
  doc.pipe(stream);

  for (let index = 0; index < pages.length; index += 1) {
    if (index > 0) {
      doc.addPage({ size: "A4", layout: "landscape", margin: 0 });
    }
    const currentSvgPath = pages[index];
    const svgSource = fs.readFileSync(currentSvgPath, "utf8");
    SVGtoPDF(doc, svgSource, 0, 0, {
      assumePt: true,
      width: PAGE_WIDTH,
      height: PAGE_HEIGHT,
      imageCallback: (href) => path.resolve(path.dirname(currentSvgPath), String(href || "")),
    });
  }

  doc.end();
  await new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.cedula || !args.fecha) {
    throw new Error("Usa --cedula y --fecha.");
  }

  let result;
  if (args.input_json) {
    const sourcePath = path.resolve(args.input_json);
    if (!fs.existsSync(sourcePath)) {
      throw new Error(`No existe input_json: ${sourcePath}`);
    }
    result = JSON.parse(fs.readFileSync(sourcePath, "utf8"));
  } else {
    result = await runSingle(args.cedula, args.fecha);
  }

  const outputDir =
    args.output_dir ||
    args.outputDir ||
    args["output-dir"] ||
    "output";

  const artifacts = await generatePdfFromResult({
    result,
    cedula: args.cedula,
    fecha: args.fecha,
    outputName: args.output_name || "",
    outputDir,
  });
  console.log(JSON.stringify(artifacts, null, 2));
}

async function generatePdfFromResult({ result, cedula, fecha, outputName = "", outputDir = "output" }) {
  if (!result || !result.response || !result.response.data) {
    throw new Error("Resultado invalido para generar PDF.");
  }
  if (!cedula || !fecha) {
    throw new Error("Se requiere cedula y fecha para generar PDF.");
  }

  const data = result.response.data;
  const seguros =
    data && data.coberturaSalud && data.coberturaSalud.CoberturaSeguros
      ? data.coberturaSalud.CoberturaSeguros.aseguradora || []
      : [];
  const privados = parsePrivados(data);

  const outDir = path.resolve(outputDir);
  ensureDir(outDir);
  const baseName = resolveBaseName(cedula, fecha, outputName);
  const pdfPath = path.join(outDir, `${baseName}.pdf`);
  const { svgPath, svgPaths } = generateSvgFromResult({ result, cedula, fecha, outputName, outputDir });
  await renderPdfFromSvg({ svgPath, svgPaths, pdfPath });

  // Limpiar archivos SVG intermedios (solo conservamos el PDF)
  for (const svg of svgPaths) {
    try {
      fs.unlinkSync(svg);
    } catch (_) {
      // ignorar si no existe
    }
  }

  return { pdfPath };
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  safeFilename,
  generatePdfFromResult,
};
