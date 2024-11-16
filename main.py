import boto3
import csv
import pandas as pd

# Inicializa el cliente de S3
s3_client = boto3.client("s3")

def validate_buckets_with_tags(bucket_list_csv, output_csv, tag_key, valid_values):
    # Lee los buckets desde el archivo CSV
    bucket_df = pd.read_csv(bucket_list_csv)
    bucket_names = bucket_df["BucketName"].tolist()

    # Crear la lista para guardar los resultados
    results = []

    # Lista los buckets de la cuenta
    all_buckets = s3_client.list_buckets()
    print("Iterando sobre los buckets...")

    for bucket in all_buckets["Buckets"]:
        bucket_name = bucket["Name"]
        tag_status = "no tag"
        tag_value = ""
        
        if bucket_name in bucket_names:
            try:
                # Obtener tags del bucket
                tagging = s3_client.get_bucket_tagging(Bucket=bucket_name)
                tags = {tag["Key"]: tag["Value"] for tag in tagging["TagSet"]}
                
                # Validar la presencia del tag
                if tag_key in tags:
                    tag_status = "yes"
                    tag_value = tags[tag_key] if tags[tag_key] in valid_values else "invalid value"
                else:
                    print(f"Bucket {bucket_name} no tiene el tag {tag_key}.")
            
            except s3_client.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchTagSet":
                    print(f"Bucket {bucket_name} no tiene tags configurados.")
                else:
                    print(f"Error al obtener tags del bucket {bucket_name}: {e}")
        
            results.append({"BucketName": bucket_name, "HasTag": tag_status, "TagValue": tag_value})

    # Guardar los resultados en un archivo CSV
    output_df = pd.DataFrame(results)
    output_df.to_csv(output_csv, index=False)
    print(f"Resultados guardados en {output_csv}.")

def main():
    # Configuración
    first_input_csv = "buckets-tag1.csv"  # CSV con la lista de buckets para validar tag1
    first_output_csv = "buckets-tag1-validation.csv"
    tag1_valid_values = ["critical", "noncritical"]  # Valores válidos para el tag tag1
    
    second_input_csv = "buckets-tag2.csv"  # CSV con la lista de buckets para validar tag2
    second_output_csv = "buckets-tag2-validation.csv"
    tag2_valid_values = ["critical", "noncritical"]  # Valores válidos para el tag tag2
    
    # Validar con el tag ""
    validate_buckets_with_tags(first_input_csv, first_output_csv, "company:integrity", tag1_valid_values)
    
    # Validar con el tag ""
    validate_buckets_with_tags(second_input_csv, second_output_csv, "company:availability", tag2_valid_values)

if __name__ == "__main__":
    main()
