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
