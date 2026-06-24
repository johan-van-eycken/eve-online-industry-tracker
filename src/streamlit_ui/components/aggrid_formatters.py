from typing import Any


def js_eu_number_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
            if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
            const n = Number(params.value);
            if (isNaN(n)) return \"\";
            return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
            }}
        """
    )


def js_eu_isk_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
            if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
            const n = Number(params.value);
            if (isNaN(n)) return \"\";
            return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
            }}
        """
    )


def js_eu_pct_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
                if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
                const n = Number(params.value);
                if (isNaN(n)) return \"\";
                return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + '%';
            }}
        """
    )


def js_icon_cell_renderer(*, JsCode: Any, size_px: int = 24) -> Any:
    """Return a DOM-element-based AG Grid cellRenderer for icon/image URL columns.

    Using a DOM element (instead of returning an HTML string) avoids cases where
    AG Grid (or st_aggrid) escapes HTML and shows the literal `<img ...>` text.
    """

    if JsCode is None:
        return None

    size = int(size_px)
    return JsCode(
        f"""
            (function() {{
                function IconRenderer() {{}}

                IconRenderer.prototype.init = function(params) {{
                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'center';
                    this.eGui.style.width = '100%';

                    var url = (params && params.value) ? String(params.value) : '';
                    if (!url) {{
                        this.eImg = null;
                        return;
                    }}

                    var img = document.createElement('img');
                    img.style.width = '{size}px';
                    img.style.height = '{size}px';
                    img.style.objectFit = 'contain';
                    img.style.display = 'block';
                    img.onerror = function() {{
                        try {{ this.style.display = 'none'; }} catch (e) {{}}
                    }};
                    img.src = url;

                    this.eImg = img;
                    this.eGui.appendChild(img);
                }};

                IconRenderer.prototype.getGui = function() {{
                    return this.eGui;
                }};

                IconRenderer.prototype.refresh = function(params) {{
                    try {{
                        var url = (params && params.value) ? String(params.value) : '';
                        if (!url) {{
                            if (this.eImg) this.eImg.style.display = 'none';
                            return true;
                        }}

                        if (!this.eImg) {{
                            this.eImg = document.createElement('img');
                            this.eImg.style.width = '{size}px';
                            this.eImg.style.height = '{size}px';
                            this.eImg.style.objectFit = 'contain';
                            this.eImg.style.display = 'block';
                            this.eImg.onerror = function() {{
                                try {{ this.style.display = 'none'; }} catch (e) {{}}
                            }};
                            this.eGui.appendChild(this.eImg);
                        }}

                        this.eImg.style.display = 'block';
                        this.eImg.src = url;
                    }} catch (e) {{
                        // ignore
                    }}
                    return true;
                }};

                return IconRenderer;
            }})()
        """
    )


def js_margin_pct_cell_style(*, JsCode: Any) -> Any:
    """Color-coded cell style for profit margin % columns.

    negative  → red background  (loss)
    0–5 %     → amber           (thin / below typical target)
    5–15 %    → neutral
    > 15 %    → green           (healthy margin)
    """
    if JsCode is None:
        return None
    return JsCode(
        """
        function(params) {
            var style = {textAlign: 'right'};
            var v = params.value;
            if (v === null || v === undefined || v === '') return style;
            var n = Number(v);
            if (isNaN(n)) return style;
            if (n < 0) {
                style.backgroundColor = '#c0392b';
                style.color = '#ffffff';
                style.fontWeight = 'bold';
            } else if (n < 5) {
                style.backgroundColor = '#e67e22';
                style.color = '#ffffff';
            } else if (n >= 15) {
                style.color = '#27ae60';
                style.fontWeight = '600';
            }
            return style;
        }
        """
    )


def js_flag_text_style(
    *,
    JsCode: Any,
    flag_field: str,
    align: str | None = None,
    color: str = "#ef4444",
    font_weight: int = 600,
) -> Any:
    if JsCode is None:
        return None

    align_value = str(align) if align else ""
    return JsCode(
        f"""
            function(params) {{
                var baseStyle = {{}};
                if ('{align_value}') {{
                    baseStyle.textAlign = '{align_value}';
                }}

                var data = (params && params.data) ? params.data : null;
                var isFlagged = Boolean(data && data['{str(flag_field)}']);
                if (!isFlagged) {{
                    return baseStyle;
                }}

                baseStyle.color = '{str(color)}';
                baseStyle.fontWeight = '{int(font_weight)}';
                return baseStyle;
            }}
        """
    )


def js_category_text_style(
    *,
    JsCode: Any,
    category_field: str = "category",
    align: str | None = None,
    income_color: str = "#22c55e",
    expense_color: str = "#ef4444",
    font_weight: int = 600,
) -> Any:
    if JsCode is None:
        return None

    align_value = str(align) if align else ""
    return JsCode(
        f"""
            function(params) {{
                var style = {{}};
                if ('{align_value}') {{
                    style.textAlign = '{align_value}';
                }}

                var row = (params && params.data) ? params.data : null;
                var category = row ? row['{str(category_field)}'] : null;
                if (category === 'Income') {{
                    style.color = '{str(income_color)}';
                    style.fontWeight = '{int(font_weight)}';
                }} else if (category === 'Expenses') {{
                    style.color = '{str(expense_color)}';
                    style.fontWeight = '{int(font_weight)}';
                }}
                return style;
            }}
        """
    )
