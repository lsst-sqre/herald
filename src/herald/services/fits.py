"""Conversion of deserialised Avro alert records to FITS format."""

from __future__ import annotations

import importlib.resources
import io
from typing import Any

import numpy as np
import yaml
from astropy.io import fits

from ..constants import IAU_ALERT_PREFIX

__all__ = ["alert_to_fits", "cutouts_to_fits"]

# Map from alert record field name to FITS EXTNAME for cutout image fields.
_CUTOUT_FIELDS: dict[str, str] = {
    "cutoutDifference": "DIFFIM",
    "cutoutScience": "SCIENCE",
    "cutoutTemplate": "TEMPLATE",
}

# Canonical EXTNAME for known top-level record/array fields.
# Fields not listed here that end up as top-level binary tables
# use the uppercased field name as its EXTNAME
_TABLE_HDU_NAMES: dict[str, str] = {
    "prvDiaForcedSources": "FORCEDPHOT",
    "ssSource": "SSSOURCE",
}

# Top-level record/array fields merged into the ALERT HDU as additional
# columns rather than getting their own BinTableHDU.
# We currently know that only one of these branches will show up for an alert
# i.e. diaObject or ssObject + mpc_orbits.
#
# ssObject is a future field not yet present in v10_0, but will likely show up
# in later schema versions.
_ALERT_MERGED_FIELDS: tuple[str, ...] = ("diaObject", "ssObject", "mpc_orbits")

# Top-level fields that are handled specially and must be excluded from the
# generic BinTableHDU loop.
_SPECIAL_FIELDS: frozenset[str] = frozenset(
    {"diaSource", "prvDiaSources", *_ALERT_MERGED_FIELDS, *_CUTOUT_FIELDS}
)

# Column units loaded from the bundled resource file.
_COLUMN_UNITS: dict[str, str] = yaml.safe_load(
    importlib.resources.files("herald.resources")
    .joinpath("column_units.yaml")
    .read_text()
)

# Null sentinels for FITS integer columns (TNULLn).
_INT32_NULL = int(np.iinfo(np.int32).min)
_INT64_NULL = int(np.iinfo(np.int64).min)
_BOOL_NULL = 255  # stored as uint8; 0=False, 1=True, 255=null


def _collect_named_types(avro_type: Any) -> dict[str, list[dict[str, Any]]]:
    """Walk an Avro type tree and return a map of record name -> fields.

    Parameters
    ----------
    avro_type
        An Avro type.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        Mapping from record name to list of field dicts.
    """
    named: dict[str, list[dict[str, Any]]] = {}
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "record":
            name = avro_type.get("name", "")
            fields = avro_type.get("fields", [])
            if name:
                named[name] = fields
            for field in fields:
                named.update(_collect_named_types(field.get("type")))
        elif avro_type.get("type") == "array":
            named.update(_collect_named_types(avro_type.get("items")))
    elif isinstance(avro_type, list):
        for t in avro_type:
            named.update(_collect_named_types(t))
    return named


def _unwrap_fields(
    avro_type: Any, named_types: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    """Return the record fields for an Avro type.

    Parameters
    ----------
    avro_type
        An Avro type.
    named_types
        Mapping from record name to list of field dicts, as returned by
        ``_collect_named_types``.

    Returns
    -------
    list[dict[str, Any]]
        List of field dicts if the type is or resolves to a record, or an empty
        list.
    """
    if isinstance(avro_type, list):
        for t in avro_type:
            if t != "null":
                fields = _unwrap_fields(t, named_types)
                if fields:
                    return fields
        return []
    if isinstance(avro_type, dict):
        if avro_type.get("type") == "record":
            return avro_type.get("fields", [])
        if avro_type.get("type") == "array":
            return _unwrap_fields(avro_type.get("items"), named_types)
    if isinstance(avro_type, str) and avro_type in named_types:
        return named_types[avro_type]
    return []


def _is_array_type(avro_type: Any) -> bool:
    """Return True if the type resolves (through a nullable union) to an array.

    Parameters
    ----------
    avro_type
        An Avro type.
    """
    if isinstance(avro_type, list):
        for t in avro_type:
            if t != "null":
                return _is_array_type(t)
        return False
    if isinstance(avro_type, dict):
        return avro_type.get("type") == "array"
    return False


def _make_fits_column(
    name: str, avro_type: Any, values: list[Any]
) -> fits.Column | None:
    """Build a FITS Column from an Avro field type and row values.

    Returns None for bytes fields (cutouts are handled separately) and for
    complex/reference types that are serialised as their own tables.
    Unit (TUNITn) is set from ``_COLUMN_UNITS`` when available.

    Parameters
    ----------
    name
        Column name, taken from the Avro field name.
    avro_type
        Avro type for the field.
    values
        List of values for this column, extracted from the alert record for all
        rows (e.g. diaSource + prvDiaSources).

    Returns
    -------
    fits.Column | None
        A FITS Column object if the type is supported, or None if the field
        should be skipped.
    """
    unit = _COLUMN_UNITS.get(name)
    nullable = False
    base_type = avro_type

    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        nullable = "null" in avro_type
        base_type = non_null[0] if non_null else None

    if base_type in (None, "bytes") or not isinstance(base_type, str):
        return None

    if base_type == "float":
        arr = np.array(
            [
                np.float32("nan") if v is None else np.float32(v)
                for v in values
            ],
            dtype=np.float32,
        )
        return fits.Column(name=name, format="E", array=arr, unit=unit)

    if base_type == "double":
        arr = np.array(
            [
                np.float64("nan") if v is None else np.float64(v)
                for v in values
            ],
            dtype=np.float64,
        )
        return fits.Column(name=name, format="D", array=arr, unit=unit)

    if base_type == "int":
        arr = np.array(
            [_INT32_NULL if v is None else int(v) for v in values],
            dtype=np.int32,
        )
        return fits.Column(
            name=name,
            format="J",
            array=arr,
            unit=unit,
            null=_INT32_NULL if nullable else None,
        )

    if base_type == "long":
        arr = np.array(
            [_INT64_NULL if v is None else int(v) for v in values],
            dtype=np.int64,
        )
        return fits.Column(
            name=name,
            format="K",
            array=arr,
            unit=unit,
            null=_INT64_NULL if nullable else None,
        )

    if base_type == "boolean":
        if nullable:
            arr = np.array(
                [_BOOL_NULL if v is None else int(v) for v in values],
                dtype=np.uint8,
            )
            return fits.Column(
                name=name, format="B", array=arr, null=_BOOL_NULL
            )
        arr = np.array([bool(v) for v in values], dtype=bool)
        return fits.Column(name=name, format="L", array=arr)

    if base_type == "string":
        str_vals = ["" if v is None else str(v) for v in values]
        width = max(max((len(s) for s in str_vals), default=0), 1)
        return fits.Column(
            name=name, format=f"{width}A", array=np.array(str_vals)
        )

    return None


def _records_to_columns(
    rows: list[dict[str, Any]], schema_fields: list[dict[str, Any]]
) -> list[fits.Column]:
    """Convert a list of Avro record dicts to a FITS column list.

    Parameters
    ----------
    rows
        List of dicts representing Avro records
        (e.g. diaSource + prvDiaSources).
    schema_fields
        List of field dicts from the Avro schema for this record type, used to
        determine column names and types.

    Returns
    -------
    list[fits.Column]
        List of FITS Column objects for the fields in the schema, with values
        extracted from the input records. Fields that are bytes or complex
        types are skipped (handled separately as cutout images or their own
        tables).
    """
    columns = []
    for field in schema_fields:
        values = [row[field["name"]] for row in rows]
        col = _make_fits_column(field["name"], field["type"], values)
        if col is not None:
            columns.append(col)
    return columns


def _build_diasource_hdu(
    record: dict[str, Any],
    dia_fields: list[dict[str, Any]],
) -> fits.BinTableHDU:
    """Build the DIASOURCE BinTableHDU.

    Row 0 is the triggering diaSource and subsequent rows are prvDiaSources.
    ``psfFlux`` is moved to immediately follow ``midpointMjdTai`` when both
    columns are present, to facilitate default light-curve plots.
    Extra columns ``trigger`` and ``iau_id`` are appended.

    Parameters
    ----------
    record
        The full alert record dict used to extract the diaSource and
        prvDiaSources.
    dia_fields
        List of field dicts from the diaSource schema, used to extract the
        diaSource fields for all rows.

    Returns
    -------
    fits.BinTableHDU
        A BinTableHDU containing the diaSource and prvDiaSources, with extra
        columns for the trigger flag and IAU identifier.
    """
    triggering = record["diaSource"]
    prv = record.get("prvDiaSources") or []
    rows = [triggering, *prv]
    columns = _records_to_columns(rows, dia_fields)
    col_names = [c.name for c in columns]

    if "psfFlux" in col_names and "midpointMjdTai" in col_names:
        psf_idx = col_names.index("psfFlux")
        mjd_idx = col_names.index("midpointMjdTai")

        # We want psfFlux to be immediately after midpointMjdTai (Apparently
        # this is a common convention for light-curve tables
        # and some plotting tools) so we remove it from its current position
        # and re-insert it after midpointMjdTai. We need to adjust the
        # insertion index if psfFlux was originally before midpointMjdTai.

        if psf_idx != mjd_idx + 1:
            psf_col = columns.pop(psf_idx)
            if psf_idx < mjd_idx:
                mjd_idx -= 1
            columns.insert(mjd_idx + 1, psf_col)

    trigger_arr = np.array([True, *([False] * len(prv))], dtype=bool)
    columns.append(fits.Column(name="trigger", format="L", array=trigger_arr))

    iau_ids = [f"{IAU_ALERT_PREFIX}{row['diaSourceId']}" for row in rows]
    width = max(len(s) for s in iau_ids)
    columns.append(
        fits.Column(name="iau_id", format=f"{width}A", array=np.array(iau_ids))
    )

    return fits.BinTableHDU.from_columns(columns, name="DIASOURCE")


def _build_optional_record_hdu(
    data: dict[str, Any] | None,
    schema_fields: list[dict[str, Any]],
    extname: str,
) -> fits.BinTableHDU:
    """Build a BinTableHDU for a record that may be null (0 or 1 rows).

    Parameters
    ----------
    data
       The record dict, or None if the field is null.
    schema_fields
         List of field dicts from the Avro schema for this record type, used to
         determine column names and types.
    extname
        EXTNAME for the HDU.

    Returns
    -------
    fits.BinTableHDU
        A BinTableHDU containing one row if the record is present, or zero rows
    """
    rows = [data] if data is not None else []
    columns = _records_to_columns(rows, schema_fields)
    return fits.BinTableHDU.from_columns(columns, name=extname)


def _build_array_hdu(
    data: list[dict[str, Any]] | None,
    schema_fields: list[dict[str, Any]],
    extname: str,
) -> fits.BinTableHDU:
    """Build a BinTableHDU for an array field (0 or more rows).

    Parameters
    ----------
    data
        List of record dicts, or None if the field is null.
    schema_fields
        List of field dicts from the Avro schema for this record type used to
        determine column names and types.
    extname
        EXTNAME for the HDU.

    Returns
    -------
    fits.BinTableHDU
        A BinTableHDU containing one row per record in the array, or zero rows
    """
    rows = data or []
    columns = _records_to_columns(rows, schema_fields)
    return fits.BinTableHDU.from_columns(columns, name=extname)


def _build_alert_hdu(
    record: dict[str, Any],
    top_fields: list[dict[str, Any]],
    named_types: dict[str, list[dict[str, Any]]],
) -> fits.BinTableHDU:
    """Build a one-row BinTableHDU for the ALERT extension.

    Contains the top-level scalar alert fields concatenated with columns
    from whichever of ``diaObject``, ``ssObject``, and ``mpc_orbits`` are
    present in the record.  Only one branch is expected per alert: diaObject
    or ssObject + mpc_orbits. Absent optional records are excluded rather than
    using null values for those fields.

    Parameters
    ----------
    record
        The full alert record dict.
    top_fields
        List of field dicts from the top-level alert schema.
    named_types
        Mapping from record name to field list, as returned by
        ``_collect_named_types``.

    Returns
    -------
    fits.BinTableHDU
        A one-row BinTableHDU.
    """
    columns = _records_to_columns([record], top_fields)

    for fname in _ALERT_MERGED_FIELDS:
        avro_type = next(
            (f["type"] for f in top_fields if f["name"] == fname), None
        )
        if avro_type is None:
            continue  # field not present in this schema version
        sub_fields = _unwrap_fields(avro_type, named_types)
        if not sub_fields:
            continue
        data = record.get(fname)
        if data is None:
            continue
        # All current merged fields are nullable records, so data is a dict.
        # The list branch is being defensive for future array fields.
        row = data[0] if isinstance(data, list) else data
        columns.extend(_records_to_columns([row], sub_fields))

    return fits.BinTableHDU.from_columns(columns, name="ALERT")


def _make_primary_hdu() -> fits.PrimaryHDU:
    """Return an empty primary HDU with standard Rubin headers.

    Returns
    -------
    fits.PrimaryHDU
        An empty primary HDU with TELESCOP and INSTRUME headers
        set for Rubin.
    """
    hdu = fits.PrimaryHDU()
    hdu.header["TELESCOP"] = "Rubin Observatory"
    hdu.header["INSTRUME"] = "LSSTCam"
    return hdu


def alert_to_fits(
    schema_dict: dict[str, Any], record: dict[str, Any]
) -> bytes:
    """Convert a deserialised alert record to a multi-extension FITS file.

    Assembles a FITS file containing image HDUs for each cutout stamp,
    a one-row ALERT BinTableHDU for top-level scalar fields, and a
    BinTableHDU for every top-level field whose Avro type resolves to a
    record or array-of-records.  The DIASOURCE HDU is handled specially
    (see ``_build_diasource_hdu``).  Known fields use the canonical
    EXTNAMEs in ``_TABLE_HDU_NAMES`` and for unrecognized fields we use
    the uppercased field name.

    Parameters
    ----------
    schema_dict
        The raw Avro schema as a Python dict, used to determine column types.
    record
        The deserialised alert record.

    Returns
    -------
    bytes
        FITS file bytes.

    Raises
    ------
    ValueError
        If required fields are missing from the schema.
    """
    top_fields = schema_dict["fields"]
    named_types = _collect_named_types(schema_dict)

    def _fields_for(field_name: str) -> list[dict[str, Any]]:
        avro_type = next(
            (f["type"] for f in top_fields if f["name"] == field_name),
            None,
        )
        if avro_type is None:
            raise ValueError(
                f"Required field '{field_name}' not found in alert schema"
            )
        return _unwrap_fields(avro_type, named_types)

    hdus: list[fits.HDU] = [_make_primary_hdu()]
    hdus.append(_build_alert_hdu(record, top_fields, named_types))

    for field, extname in _CUTOUT_FIELDS.items():
        data = record.get(field)
        if data is not None:
            with fits.open(io.BytesIO(data)) as stamp:
                hdus.append(
                    fits.ImageHDU(
                        data=stamp[0].data,
                        header=stamp[0].header,
                        name=extname,
                    )
                )

    hdus.append(_build_diasource_hdu(record, _fields_for("diaSource")))

    for field in top_fields:
        fname = field["name"]
        if fname in _SPECIAL_FIELDS:
            continue
        sub_fields = _unwrap_fields(field["type"], named_types)
        if not sub_fields:
            continue
        extname = _TABLE_HDU_NAMES.get(fname, fname.upper())
        data = record.get(fname)
        if _is_array_type(field["type"]):
            hdus.append(_build_array_hdu(data, sub_fields, extname))
        else:
            hdus.append(_build_optional_record_hdu(data, sub_fields, extname))

    buf = io.BytesIO()
    fits.HDUList(hdus).writeto(buf)
    return buf.getvalue()


def cutouts_to_fits(record: dict[str, Any]) -> bytes:
    """Extract cutout stamp images from an alert record as a FITS file.

    Parameters
    ----------
    record
        The deserialised alert record.

    Returns
    -------
    bytes
        FITS file containing one image extension per available cutout stamp,
        or just a primary HDU if none are present.
    """
    hdus: list[fits.HDU] = [fits.PrimaryHDU()]
    for field, extname in _CUTOUT_FIELDS.items():
        data = record.get(field)
        if data is None:
            continue
        with fits.open(io.BytesIO(data)) as stamp:
            hdus.append(
                fits.ImageHDU(
                    data=stamp[0].data,
                    header=stamp[0].header,
                    name=extname,
                )
            )

    buf = io.BytesIO()
    fits.HDUList(hdus).writeto(buf)
    return buf.getvalue()
