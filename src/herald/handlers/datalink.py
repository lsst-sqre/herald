"""DataLink VOTable builder for alert data products."""

import xml.etree.ElementTree as ET

__all__ = ["build_links_votable"]

_VOTABLE_NS = "http://www.ivoa.net/xml/VOTable/v1.3"
_AVRO_OCF_CONTENT_TYPE = "application/x-avro-ocf"
_FITS_CONTENT_TYPE = "application/fits"

ET.register_namespace("", _VOTABLE_NS)


def build_links_votable(alert_id: int, service_base_url: str) -> bytes:
    """Build a DataLinks VOTable listing available data products
    for a given alert.

    Parameters
    ----------
    alert_id
        The numeric alert ID.
    service_base_url
        The base URL of this service used to construct access
        URLs for each data product.

    Returns
    -------
    bytes
        UTF-8 encoded VOTable XML.
    """
    ns = _VOTABLE_NS

    votable = ET.Element(f"{{{ns}}}VOTABLE", attrib={"version": "1.4"})
    resource = ET.SubElement(
        votable, f"{{{ns}}}RESOURCE", attrib={"type": "results"}
    )
    table = ET.SubElement(resource, f"{{{ns}}}TABLE")

    for name, datatype, arraysize, ucd in (
        ("ID", "char", "*", "meta.id;meta.main"),
        ("access_url", "char", "*", "meta.ref.url"),
        ("service_def", "char", "*", "meta.ref"),
        ("error_message", "char", "*", "meta.code.error"),
        ("description", "char", "*", "meta.note"),
        ("semantics", "char", "*", "meta.code"),
        ("content_type", "char", "*", "meta.code.mime"),
        ("content_length", "long", None, "phys.size;meta.file"),
    ):
        attrib: dict[str, str] = {
            "name": name,
            "datatype": datatype,
            "ucd": ucd,
        }
        if arraysize:
            attrib["arraysize"] = arraysize
        ET.SubElement(table, f"{{{ns}}}FIELD", attrib=attrib)

    data = ET.SubElement(table, f"{{{ns}}}DATA")
    tabledata = ET.SubElement(data, f"{{{ns}}}TABLEDATA")

    str_id = str(alert_id)
    for access_url, description, semantics, content_type in (
        (
            f"{service_base_url}?ID={alert_id}",
            "Alert packet (Avro OCF)",
            "#this",
            _AVRO_OCF_CONTENT_TYPE,
        ),
        (
            f"{service_base_url}?ID={alert_id}&RESPONSEFORMAT=fits",
            "Alert packet (FITS)",
            "#this",
            _FITS_CONTENT_TYPE,
        ),
        (
            f"{service_base_url}/cutouts?ID={alert_id}",
            "Cutout images (FITS)",
            "#cutout",
            _FITS_CONTENT_TYPE,
        ),
        (
            f"{service_base_url}/schema?ID={alert_id}",
            "Avro schema (JSON)",
            "#detached-header",
            "application/json",
        ),
    ):
        tr = ET.SubElement(tabledata, f"{{{ns}}}TR")
        for value in (
            str_id,
            access_url,
            "",
            "",
            description,
            semantics,
            content_type,
            "",
        ):
            td = ET.SubElement(tr, f"{{{ns}}}TD")
            td.text = value

    return ET.tostring(votable, encoding="utf-8", xml_declaration=True)
