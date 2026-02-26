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
