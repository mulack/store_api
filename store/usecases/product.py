from typing import List, Optional
from uuid import UUID
import pymongo
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from store.models.product import ProductModel
from store.schemas.product import ProductIn, ProductOut, ProductUpdate, ProductUpdateOut
from store.core.exceptions import NotFoundException
from datetime import datetime
from bson import ObjectId

class ProductUsecase:
    def __init__(self) -> None:
        self.client: AsyncIOMotorClient = db_client.get()
        self.database: AsyncIOMotorDatabase = self.client.get_database()
        self.collection = self.database.get_collection("products")

    async def create(self, body: ProductIn) -> ProductOut:
        product_model = ProductModel(**body.model_dump())
        await self.collection.insert_one(product_model.model_dump())

        return ProductOut(**product_model.model_dump())

    async def get(self, id: UUID) -> ProductOut:
        result = await self.collection.find_one({"id": id})

        if not result:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        return ProductOut(**result)

    async def query(self) -> List[ProductOut]:
        products = []
        async for item in self.collection.find():
            products.append(ProductOut(**item))
        return products

    async def update(self, id: UUID, body: ProductUpdate) -> ProductUpdateOut:
        result = await self.collection.find_one_and_update(
            filter={"id": id},
            update={"$set": body.model_dump(exclude_none=True, updated_at=datetime.utcnow())},
            return_document=pymongo.ReturnDocument.AFTER,
        )

        if not result:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        return ProductUpdateOut(**result)

    async def delete(self, id: UUID) -> bool:
        result = await self.collection.delete_one({"id": id})
        if result.deleted_count == 0:
            raise NotFoundException(message=f"Product not found with filter: {id}")

        return True

    async def filter_by_price(self, min_price: Optional[float], max_price: Optional[float]) -> List[ProductOut]:
        filter_query = {}

        if min_price is not None:
            filter_query["price"] = {"$gt": min_price}

        if max_price is not None:
            filter_query["price"] = {"$lt": max_price}

        products = []
        async for item in self.collection.find(filter_query):
            products.append(ProductOut(**item))
        
        return products
