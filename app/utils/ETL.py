from json import dumps
from awsglue.dynamicframe import DynamicFrame

def create_frame_from_catalog(parameters:dict, fields_to_keep:list, ctx):
    '''Creates Glue DynamicFrame object from parameters.

    Parameters
    ----------
    parameters : dict
        The json containing the parameters for the creation and normalization of the glue DynamicFrame.

    Returns
    -------
    Glue DynamicFrame Object
        A DynamicFrame with the desired field mapping.
    '''
    # For time being please lookup https://discuss.python.org/t/dynamic-variable-name/18585/7 in case theres any hickups
    # https://www.programiz.com/python-programming/methods/built-in/locals
    temp_dyf = ctx.create_dynamic_frame.from_catalog(
        database=parameters['database'],
        table_name=parameters['table_name'],
        transformation_ctx=f"{parameters['prefix']}_dyf",
    )
    return normalize_column_names(temp_dyf, parameters, fields_to_keep)

def normalize_column_names(temp_dyf, parameters:dict, fields_to_keep:list):
    '''Maps the desired fields for the DynamicFrame and rename them for ease of access.

    Parameters
    ----------
    temp_dyf : Glue DynamicFrame Object
        The variable where the the DynamicFrame schema and rows are stored at.
    parameters : dict
        The json containing the parameters for the creation and normalization of the glue DynamicFrame.

    Returns
    -------
    Glue DynamicFrame Object
        A DynamicFrame with the desired field mapping.
    '''
    mappings=[]
    if parameters['mappings'] is not None:
        return temp_dyf.apply_mapping(parameters['mappings'])
    else:
        for line in temp_dyf.schema().field_map:
            if line in fields_to_keep:
                mappings.append((line, f"{parameters['prefix']}{line}"))
        return temp_dyf.apply_mapping(mappings)

def repartition_frames(dyf, numpart:int):
    '''Splits DynamicFrame into many file partitions (helps on lambda processing).

    Parameters
    ----------
    dyf : Glue DynamicFrame Object
        The variable where the the DynamicFrame schema and rows are stored at.
    numpart : number
        The ammount of partitions to split the code.

    Returns
    -------
    Splited Glue DynamicFrame Object
        A DynamicFrame with the desired field mapping.
    '''
    return dyf.repartition(numpart)

def write_dynamic_frame(dyf, partitionkeys:list, format:str, compression:str, path, gluectx):
    '''Writes the output of the ETL job into an s3 bucket.

    Parameters
    ----------
    dyf : Glue DynamicFrame Object
        The variable where the the DynamicFrame schema and rows are stored at.
    partitionkeys : list
        The path structure that will be used to store data on s3 
    fmt : str
        The format that data will be stores (JSON | ORC | CSV | PARQUET) 
    compression : str
        Compression type (ZIP | GZIP | BZIP)
    path : list
        The path structure that will be used to store data on s3 
    ctx : GlueContext
        GlueContext for PySpark Jobs

    Returns
    -------
    Splited Glue DynamicFrame Object
        A DynamicFrame with the desired field mapping.
    '''
    return gluectx.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format=format,
        connection_options={
            "path": path,
            "compression": compression,
            "groupFiles": "inPartition",
            "groupSize": "1485760",
            "partitionKeys": partitionkeys,
        },
        transformation_ctx="AWSAccounts_Output_DF",
    )

def deduplicate_rows(dyf, gluectx):
    return DynamicFrame.fromDF(dyf.toDF().dropDuplicates(), gluectx, "DropDuplicates")
