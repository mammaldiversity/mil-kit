"""
Merge metadata with mdd metadata then export as JSON.
"""


from pathlib import Path
import polars as pl
import re

MIL_COL = ["milNo", "genus", "specificEpithet", "descriptionOfImage", "photographer", "locationWhereImageTaken"]
MDD_COLS = ["id", "genus", "specificEpithet"]


class MetadataForMdd:
    def __init__(self, mil_path: Path, mdd_path: Path) -> None:
        self.mil_path = mil_path
        self.mdd_path = mdd_path

    def to_json(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Add json extension if not present
        if output_path.suffix.lower() != ".json":
            output_path = output_path.with_suffix(output_path.suffix + ".json")
        merged_df = self._load_metadata()
        merged_df.write_json(output_path)

    def _load_metadata(self) -> pl.DataFrame:
        mil_df = self._load_file(self.mil_path) 
        mdd_df = self._load_file(self.mdd_path)  
        mil_df = self._clean_column_names(mil_df)
        mdd_df = self._clean_column_names(mdd_df)

        mil_df = (
            mil_df.select(MIL_COL)
            .rename({
                "milNo": "milId",
                "descriptionOfImage": "description",
                "locationWhereImageTaken": "location",
            })
            .with_columns(
                (pl.col("genus") + "_" + pl.col("specificEpithet")).alias("scientificName")
            )
            .drop(["genus", "specificEpithet"]) 
        )

        mdd_df = (
            mdd_df.select(MDD_COLS)
            .rename({
                "id": "mddId",
            })
            .with_columns(
                (pl.col("genus") + "_" + pl.col("specificEpithet")).alias("scientificName")
            )
            .drop(["genus", "specificEpithet"])
        )

        merged_df = mil_df.join(mdd_df, on="scientificName", how="left")
        return merged_df.drop("scientificName")


    def _clean_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean column names by stripping whitespace, replacing '#' with 'No',
        removing special characters, and converting to camelCase.

        Args:
            df (pl.DataFrame): Input dataframe with raw column names.

        Returns:
            pl.DataFrame: Dataframe with cleaned column names.
        """
        def to_camel(col: str) -> str:
            col = col.strip().replace("#", "No")
            col = re.sub(r"[^\w\s]", "", col)     
            words = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).split()
            return words[0].lower() + "".join(w.capitalize() for w in words[1:])

        return df.rename(to_camel)



    def _load_file(self, file_path: Path) -> pl.DataFrame:
        """
        Dispatch file loading based on the file extension.

        Polars reads all columns as strings (``infer_schema_length=0``) to
        prevent MIL numbers from being silently cast to floats or integers,
        which would break string-based key matching.

        Returns:
            pl.DataFrame: Raw dataframe from the source file.
        """
        suffix = file_path.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            return pl.read_excel(file_path, infer_schema_length=0)
        return pl.read_csv(file_path, infer_schema_length=0)
