from PIL import Image, ImageDraw
import math, os

S = 256
OUT = "assets"
os.makedirs(OUT, exist_ok=True)

BLUE  = (46, 109, 184, 255)
GREEN = (42, 143, 79, 255)
RED   = (196, 62, 52, 255)
AMBER = (222, 148, 30, 255)
PAPER = (233, 239, 247, 255)
EDGE  = (58, 92, 138, 255)
WHITE = (255, 255, 255, 255)

def canvas():
    im = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    return im, ImageDraw.Draw(im)

def doc(d, x, y, w, h, wd=10):
    fold = w * 0.34
    body = [(x, y), (x + w - fold, y), (x + w, y + fold), (x + w, y + h), (x, y + h)]
    d.polygon(body, fill=PAPER)
    d.line(body + [body[0]], fill=EDGE, width=wd, joint="curve")
    d.line([(x + w - fold, y), (x + w - fold, y + fold), (x + w, y + fold)], fill=EDGE, width=wd)

def badge(d, cx, cy, r, color, glyph, ring=True):
    if ring:
        d.ellipse([cx - r * 1.14, cy - r * 1.14, cx + r * 1.14, cy + r * 1.14], fill=WHITE)
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=color)
    if glyph == "up":
        a = r * 0.54
        d.polygon([(cx, cy - a * 1.1), (cx - a, cy + a * 0.1), (cx + a, cy + a * 0.1)], fill=WHITE)
        d.rectangle([cx - a * 0.36, cy, cx + a * 0.36, cy + a * 0.85], fill=WHITE)
    elif glyph == "down":
        # Треугольник вниз — тот же знак, которым платформа помечает выпадающий список.
        a = r * 0.60
        d.polygon([(cx - a, cy - a * 0.5), (cx + a, cy - a * 0.5), (cx, cy + a * 0.72)], fill=WHITE)
    else:
        a, w = r * 0.48, max(2, int(r * 0.30))
        d.line([(cx - a, cy - a), (cx + a, cy + a)], fill=WHITE, width=w)
        d.line([(cx - a, cy + a), (cx + a, cy - a)], fill=WHITE, width=w)

# --- рисовалки: small=True — упрощённый вариант для 16 px --------------------
def refresh(small):
    im, d = canvas()
    m, w = (28, 46) if small else (34, 30)
    d.arc([m, m, S - m, S - m], start=-60, end=205, fill=BLUE, width=w)
    ang = math.radians(-60); r = (S - 2 * m) / 2
    px, py = S / 2 + r * math.cos(ang), S / 2 + r * math.sin(ang)
    k = 1.5 if small else 1.0
    d.polygon([(px + 46 * k, py + 6 * k), (px - 30 * k, py + 30 * k), (px - 6 * k, py - 46 * k)], fill=BLUE)
    return im

def odata(small):
    im, d = canvas()
    m = 20 if small else 26
    d.ellipse([m, m, S - m, S - m], fill=BLUE)
    w = 20 if small else 12
    d.ellipse([S / 2 - (52 if small else 44), m + 2, S / 2 + (52 if small else 44), S - m - 2],
              outline=WHITE, width=w)
    d.line([m + 8, S / 2, S - m - 8, S / 2], fill=WHITE, width=w)
    if not small:
        d.arc([m + 4, S / 2 - 78, S - m - 4, S / 2 + 12], start=0, end=180, fill=WHITE, width=11)
        d.arc([m + 4, S / 2 - 12, S - m - 4, S / 2 + 78], start=180, end=360, fill=WHITE, width=11)
    return im

def init(small):
    im, d = canvas()
    bolt = ([(154, 12), (60, 146), (118, 146), (88, 244), (198, 100), (136, 100), (176, 12)] if small
            else [(150, 18), (74, 140), (122, 140), (96, 238), (186, 106), (134, 106), (168, 18)])
    d.polygon(bolt, fill=AMBER)
    d.line(bolt + [bolt[0]], fill=AMBER, width=20 if small else 14, joint="curve")
    return im

def one_doc(small, color, glyph):
    im, d = canvas()
    if small:
        doc(d, 26, 18, 150, 186, wd=20)
        badge(d, 178, 182, 70, color, glyph)
    else:
        doc(d, 40, 26, 150, 190, wd=10)
        badge(d, 178, 186, 62, color, glyph)
    return im

def many_docs(small, color, glyph):
    im, d = canvas()
    if small:
        doc(d, 14, 12, 140, 168, wd=20)      # два листа вместо трёх — на 16 px три сливаются
        doc(d, 74, 62, 140, 168, wd=20)
        badge(d, 190, 186, 66, color, glyph)
    else:
        doc(d, 22, 14, 128, 162, wd=10)
        doc(d, 58, 44, 128, 162, wd=10)
        doc(d, 94, 74, 128, 162, wd=10)
        badge(d, 186, 190, 60, color, glyph)
    return im

def _rot(cx, cy, x, y, a):
    ca, sa = math.cos(a), math.sin(a)
    return (cx + x * ca - y * sa, cy + x * sa + y * ca)


def service(small):
    """Шестерёнка — группа служебных действий. Зубьев на 16 px меньше и они толще: на таком
    размере восемь тонких сливаются в размытое кольцо."""
    im, d = canvas()
    c = S / 2
    r = 80 if small else 76
    teeth = 6 if small else 8
    tw = r * (0.42 if small else 0.34)
    for i in range(teeth):
        a = 2 * math.pi * i / teeth
        d.polygon([_rot(c, c, x, y, a) for x, y in
                   [(r * 0.70, -tw), (r * 1.52, -tw * 0.74), (r * 1.52, tw * 0.74), (r * 0.70, tw)]],
                  fill=BLUE)
    d.ellipse([c - r, c - r, c + r, c + r], fill=BLUE)
    # Дырка в ступице: пишем прозрачные пиксели поверх — ImageDraw кладёт цвет как есть.
    hole = r * (0.46 if small else 0.40)
    d.ellipse([c - hole, c - hole, c + hole, c + hole], fill=(0, 0, 0, 0))
    return im


ICONS = {
    "refresh":       refresh,
    "odata":         odata,
    "init":          init,
    "upload-object": lambda s: one_doc(s, GREEN, "up"),
    "upload-all":    lambda s: many_docs(s, GREEN, "up"),
    "clear-object":  lambda s: one_doc(s, RED, "x"),
    "clear-all":     lambda s: many_docs(s, RED, "x"),
    # Картинки группы-подменю, куда убраны выгрузка и очистка. Два варианта на выбор:
    # "menu" держится грамматики набора (листы + бейдж), "service" отличается силуэтом.
    "menu":          lambda s: many_docs(s, BLUE, "down"),
    "service":       service,
}

# 16 не выпускаем: в обработке используются 32 px. Добавьте 16 в SIZES, если понадобится —
# для него рисуется отдельный упрощённый вариант (fn(True)).
SIZES = (32, 64)

for name, fn in ICONS.items():
    for px in SIZES:
        src = fn(px <= 16)
        src.resize((px, px), Image.LANCZOS).save(f"{OUT}/{name}-{px}.png")
print("ok")
