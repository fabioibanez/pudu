# first line: 527
def _read_parquet_nocache(
    path: str,
    version_id: Optional[str],
    columns: Optional[List[str]],
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    boto3_session: Union[boto3.Session, _utils.Boto3PrimitivesType],
    dataset: bool,
    validate_schema: Optional[bool],
    path_root: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    pyarrow_args = _set_default_pyarrow_additional_kwargs(pyarrow_additional_kwargs)
    boto3_session = _utils.ensure_session(boto3_session)
    df: pd.DataFrame = _arrowtable2df(
        table=_read_parquet_file(
            path=path,
            columns=columns,
            categories=categories,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
            version_id=version_id,
            pyarrow_additional_kwargs=pyarrow_args,
        ),
        categories=categories,
        safe=safe,
        map_types=map_types,
        use_threads=use_threads,
        dataset=dataset,
        path=path,
        path_root=path_root,
        timestamp_as_object=pyarrow_args["timestamp_as_object"],
    )
    if validate_schema and columns:
        for column in columns:
            if column not in df.columns:
                raise exceptions.InvalidArgument(f"column: {column} does not exist")
    return df
