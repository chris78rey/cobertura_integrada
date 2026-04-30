const fs = require("fs");
const path = require("path");

const PX_PER_PT = 4 / 3;

function pxToPt(valuePx) {
  return Number(valuePx) / PX_PER_PT;
}

function parseMatrix(attributeText) {
  const match = attributeText.match(/transform="matrix\(([^)]+)\)"/i);
  if (!match) {
    return null;
  }
  const values = match[1]
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item));
  if (values.length !== 6) {
    return null;
  }
  return {
    scaleX: values[0],
    scaleY: values[3],
    x: values[4],
    y: values[5],
  };
}

function parseImageTransforms(svgText) {
  const out = new Map();
  const imageTagRegex = /<image\b([^>]+)>/gi;
  let match = imageTagRegex.exec(svgText);
  while (match) {
    const attrs = match[1];
    const id = (attrs.match(/\bid="([^"]+)"/i) || [])[1];
    const matrix = parseMatrix(attrs);
    if (id && matrix) {
      out.set(id, {
        x: pxToPt(matrix.x),
        y: pxToPt(matrix.y),
        width: pxToPt(matrix.scaleX),
        height: pxToPt(matrix.scaleY),
      });
    }
    match = imageTagRegex.exec(svgText);
  }
  return out;
}

function parseHeaderLineY(svgText) {
  const pathMatch = svgText.match(/<path[^>]+id="path1"[^>]+>/i);
  if (!pathMatch) {
    return 52;
  }
  const pathTag = pathMatch[0];
  const dMatch = pathTag.match(/\bd="\s*[mM]\s*[0-9.]+\s*,\s*([0-9.]+)/i);
  const scaleMatch = pathTag.match(/transform="scale\(([^)]+)\)"/i);
  if (!dMatch) {
    return 52;
  }
  const yRaw = Number(dMatch[1]);
  const scale = scaleMatch ? Number(scaleMatch[1]) : 1;
  if (!Number.isFinite(yRaw) || !Number.isFinite(scale) || scale === 0) {
    return 52;
  }
  return pxToPt(yRaw * scale);
}

function loadSvgLayout(svgPath = path.resolve("ensvg.SVg")) {
  const fallback = {
    headerLineY: 52,
    mspLogo: { x: 20, y: 10, width: 120, height: 40 },
    escudo: { x: 780, y: 10, width: 40, height: 40 },
    rpisLogo: { x: 656, y: 59, width: 147, height: 60 },
  };

  if (!fs.existsSync(svgPath)) {
    return fallback;
  }

  try {
    const svgText = fs.readFileSync(svgPath, "utf8");
    const transforms = parseImageTransforms(svgText);
    return {
      headerLineY: parseHeaderLineY(svgText),
      mspLogo: transforms.get("image2") || fallback.mspLogo,
      escudo: transforms.get("image3") || fallback.escudo,
      rpisLogo: transforms.get("image4") || fallback.rpisLogo,
    };
  } catch {
    return fallback;
  }
}

module.exports = {
  loadSvgLayout,
};
