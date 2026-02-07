from pathlib import Path

from qgis.core import (
    QgsColorRampShader,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsRasterLayer,
    QgsRasterShader,
    QgsSingleBandPseudoColorRenderer,
)
from qgis.PyQt.QtGui import QColor


BASE = Path(r"C:\Users\orlan\Documentos\GitHub\livestock_view")
MONTH = globals().get("MONTH", "03/2025")  # Formato: MM/YYYY

m, y = MONTH.split("/")
month = int(m)
year = int(y)

snap_dirs = [
    BASE / "output" / "flood_2025" / "snapshots",
    BASE / "output" / "flood" / "snapshots",
]

snapshot = None
for d in snap_dirs:
    candidates = sorted(d.glob(f"s1_flood_diff_{year:04d}-{month:02d}-*.tif"))
    if candidates:
        snapshot = candidates[-1]
        break

if snapshot is None:
    raise FileNotFoundError(f"No snapshot found for {MONTH} in {snap_dirs}")

style_qml = BASE / "qgis" / "styles" / "flood_diff_diverging.qml"

layer = QgsRasterLayer(str(snapshot), f"Flood {year:04d}-{month:02d}", "gdal")
if not layer.isValid():
    raise RuntimeError(f"Invalid raster: {snapshot}")

QgsProject.instance().addMapLayer(layer)
QgsProject.instance().setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))

ok = False
if style_qml.exists():
    ok, msg = layer.loadNamedStyle(str(style_qml))
    print("QML:", ok, msg)

if not ok:
    shader = QgsRasterShader()
    ramp = QgsColorRampShader()
    ramp.setColorRampType(QgsColorRampShader.Interpolated)
    ramp.setColorRampItemList(
        [
            QgsColorRampShader.ColorRampItem(-3.0, QColor("#08306b"), "-3"),
            QgsColorRampShader.ColorRampItem(-2.0, QColor("#2171b5"), "-2"),
            QgsColorRampShader.ColorRampItem(-1.0, QColor("#6baed6"), "-1"),
            QgsColorRampShader.ColorRampItem(-0.5, QColor("#c6dbef"), "-0.5"),
            QgsColorRampShader.ColorRampItem(0.0, QColor("#f7f7f7"), "0"),
            QgsColorRampShader.ColorRampItem(0.5, QColor("#fddbc7"), "0.5"),
            QgsColorRampShader.ColorRampItem(1.0, QColor("#f4a582"), "1"),
            QgsColorRampShader.ColorRampItem(2.0, QColor("#d6604d"), "2"),
            QgsColorRampShader.ColorRampItem(3.0, QColor("#b2182b"), "3"),
        ]
    )
    shader.setRasterShaderFunction(ramp)
    renderer = QgsSingleBandPseudoColorRenderer(layer.dataProvider(), 1, shader)
    renderer.setOpacity(0.70)
    layer.setRenderer(renderer)

layer.triggerRepaint()
iface.mapCanvas().setExtent(layer.extent())
iface.mapCanvas().refresh()
print(f"Loaded: {snapshot}")
