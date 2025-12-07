// controllers/jerarquia/get_tree.js
import { getJerarquiaTree } from "../../services/jerarquia.get_tree.service.js";

export async function getJerarquiaTreeController(req, res) {
  try {
    const result = await getJerarquiaTree();
    return res.json(result);

  } catch (e) {
    console.error(" Error obteniendo árbol jerárquico:", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo construir la jerarquía",
      detail: e.message
    });
  }
}
