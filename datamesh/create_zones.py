# python create_zones.py --project_id gcp-data-engineer-10 --region us-central1 --lake ecommerce-lake --domains ventas marketing --zone_types raw curated

import argparse
import time
from google.cloud import storage
from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists


def create_bucket(project_id: str, region: str, bucket_name: str):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket, location=region)
        print(f"✅ Bucket creado: {bucket.name}")
    else:
        print(f"⚠️ Bucket {bucket_name} ya existe")

    return bucket.name


def create_lake(project_id: str, region: str, lake_name: str):
    client = dataplex_v1.DataplexServiceClient()
    parent = f"projects/{project_id}/locations/{region}"

    lake = dataplex_v1.Lake()
    lake.display_name = "Data Mesh Lake"
    lake.description = "Data Lake para arquitectura Data Mesh"

    try:
        operation = client.create_lake(
            parent=parent,
            lake_id=lake_name,
            lake=lake,
        )
        response = operation.result(timeout=600)
        print(f"✅ Data Lake creado: {response.name}")

    except AlreadyExists:
        print(f"⚠️ Lake ya existe: {lake_name}")

    time.sleep(5)
    return lake_name


def create_zone_and_asset(project_id: str, region: str, lake_name: str, domain: str, bucket_name: str, zone_type: str):
    client = dataplex_v1.DataplexServiceClient()

    parent = f"projects/{project_id}/locations/{region}/lakes/{lake_name}"
    zone_id = f"{zone_type}-{domain}-zone"

    # 🔹 ZONE
    zone = dataplex_v1.Zone()
    zone.display_name = f"{zone_type.capitalize()} {domain.capitalize()} Zone"
    zone.description = f"Zona {zone_type} del dominio {domain}"

    if zone_type == "raw":
        zone.type_ = dataplex_v1.Zone.Type.RAW
    else:
        zone.type_ = dataplex_v1.Zone.Type.CURATED

    zone.resource_spec = dataplex_v1.Zone.ResourceSpec()
    zone.resource_spec.location_type = dataplex_v1.Zone.ResourceSpec.LocationType.SINGLE_REGION

    try:
        operation = client.create_zone(
            parent=parent,
            zone_id=zone_id,
            zone=zone,
        )
        response = operation.result(timeout=600)
        print(f"✅ Zona creada: {response.name}")

    except Exception as e:
        if "already in use" in str(e).lower():
            print(f"⚠️ Zona ya existe: {zone_id}")
        else:
            raise e

    time.sleep(5)

    # 🔹 ASSET (SIEMPRE INTENTA CREARLO)
    asset_id = f"{zone_type}-{domain}-asset"

    asset = dataplex_v1.Asset()
    asset.display_name = f"{zone_type.capitalize()} {domain.capitalize()} Asset"
    asset.description = f"Asset {zone_type} para {domain}"

    asset.resource_spec = dataplex_v1.Asset.ResourceSpec()
    asset.resource_spec.type_ = dataplex_v1.Asset.ResourceSpec.Type.STORAGE_BUCKET
    asset.resource_spec.name = f"projects/{project_id}/buckets/{bucket_name}"

    try:
        operation_asset = client.create_asset(
            parent=f"{parent}/zones/{zone_id}",
            asset_id=asset_id,
            asset=asset,
        )
        asset_response = operation_asset.result(timeout=600)
        print(f"✅ Asset creado: {asset_response.name}")

    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"⚠️ Asset ya existe: {asset_id}")
        else:
            raise e

    time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--lake", required=True)
    parser.add_argument("--domains", nargs="+", required=True)
    parser.add_argument("--zone_types", nargs="+", required=True, help="raw, curated o ambos")

    args = parser.parse_args()

    print(f"🚀 Creando Data Lake [{args.lake}]...")
    create_lake(args.project_id, args.region, args.lake)

    for domain in args.domains:
        for ztype in args.zone_types:

            bucket_name = f"{args.project_id}-{ztype}-{domain}-zone"

            print(f"\n📦 Dominio: {domain} | Tipo: {ztype}")

            create_bucket(args.project_id, args.region, bucket_name)
            create_zone_and_asset(args.project_id, args.region, args.lake, domain, bucket_name, ztype)

    print("\n🎉 Data Mesh dinámico creado 🚀")