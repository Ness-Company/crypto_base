from datetime import datetime as dt
from datetime import timezone as tz
import math
from typing import ClassVar, Generic, Optional, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select, desc, asc
from sqlmodel import Session, SQLModel, func, select


T = TypeVar("T", bound=SQLModel)


class BaseRepository(Generic[T]):
    model: ClassVar[Optional[type[T]]] = None

    def __init__(self, session: Session):
        self.session = session

    def create(self, instance: T) -> T:
        return self._add(instance)

    def update(self, instance: T, **update_kwargs) -> T:
        if hasattr(instance, "updated_at"):
            instance.updated_at = dt.now(tz.utc)

        for key, value in update_kwargs.items():
            setattr(instance, key, value)

        return self._add(instance)

    def get_by_id(self, instance_id: int) -> Optional[T] | None:
        """
        Fetch an instance by its ID.
        """
        return self.session.exec(select(self.model).where(self.model.id == instance_id)).first()

    def get_all(
        self, exec_query: bool = True, order_by: Optional[str | list[str]] = None, **filters
    ) -> list[T] | Select[tuple[T]]:
        """
        Fetch all instances with optional filters and ordering.
        - order_by: a string or list of strings like 'created_at' or '-id'
                    prefix '-' for descending.
        If exec_query=True, returns list[T]; otherwise, returns unexecuted Select.
        """
        query = select(self.model)
        if filters:
            where_clauses = [
                getattr(self.model, field) == value
                for field, value in filters.items()
                if hasattr(self.model, field) and value is not None
            ]
            if where_clauses:
                query = query.where(*where_clauses)

        query = self._order_by(query, order_by)

        return self.session.exec(query).all() if exec_query else query

    def get_paginated(
        self,
        query: Select[T],
        limit: int = 20,
        page: int = 1,
    ) -> dict:
        limit = max(limit or 1, 1)
        page = max(page or 1, 1)

        total_count = self.session.exec(select(func.count()).select_from(query.subquery())).one()
        total_pages = math.ceil(total_count / limit)

        return {
            "items": self.session.exec(query.offset((page - 1) * limit).limit(limit)).all(),
            "next_page": page + 1 if total_pages and page < total_pages else None,
            "prev_page": page - 1 if page > 1 else None,
            "total_pages": total_pages,
            "total_count": total_count,
        }

    def _add(self, instance: T) -> T:
        try:
            self.session.add(instance)
            self.session.commit()
            self.session.refresh(instance)
        except Exception:
            self.session.rollback()
            raise

        return instance

    def _filter_query(self, query: Select[T], model=None, **filters) -> Select[T]:
        model = model or self.model

        if filters:
            filters = [
                getattr(model, field) == value
                for field, value in filters.items()
                if hasattr(model, field) and value is not None
            ]
            if filters:
                query = query.where(*filters)

        return query

    def _order_by(self, query: Select[T], order_by: Optional[str | list[str]] = None) -> Select[T]:
        if not order_by:
            return query

        if isinstance(order_by, str):
            order_by = [order_by]

        order_expressions = []
        for field in order_by:
            if not field:
                continue

            descending = field.startswith("-")
            field_name = field[1:] if descending else field
            if hasattr(self.model, field_name):
                column = getattr(self.model, field_name)
                order_expressions.append(desc(column) if descending else asc(column))

        return query.order_by(*order_expressions) if order_expressions else query


class AsyncBaseRepository(Generic[T]):
    model: ClassVar[Optional[type[T]]] = None

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, instance: T) -> T:
        return await self._add(instance)

    async def bulk_create(self, instances: list[T]) -> list[T]:
        try:
            self.session.add_all(instances)
            await self.session.commit()
            for instance in instances:
                await self.session.refresh(instance)
        except Exception:
            await self.session.rollback()
            raise

        return instances

    async def update(self, instance: T, **update_kwargs) -> T:
        if hasattr(instance, "updated_at"):
            instance.updated_at = dt.now(tz.utc)

        for key, value in update_kwargs.items():
            setattr(instance, key, value)

        return await self._add(instance)

    async def get_by_id(self, instance_id: int) -> Optional[T]:
        query = select(self.model).where(self.model.id == instance_id)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def get_all(
        self,
        exec_query: bool = True,
        order_by: Optional[str | list[str]] = None,
        **filters,
    ) -> list[T] | Select[tuple[T]]:
        query = select(self.model)

        if filters:
            where_clauses = [
                getattr(self.model, field) == value
                for field, value in filters.items()
                if hasattr(self.model, field) and value is not None
            ]
            if where_clauses:
                query = query.where(*where_clauses)

        query = self._order_by(query, order_by)

        if not exec_query:
            return query

        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_paginated(
        self,
        query,
        limit: int = 20,
        page: int = 1,
    ) -> dict:
        limit = max(limit or 1, 1)
        page = max(page or 1, 1)

        count_query = select(func.count()).select_from(query.subquery())
        total_count = (await self.session.execute(count_query)).scalar_one()
        total_pages = math.ceil(total_count / limit)

        paginated_query = query.offset((page - 1) * limit).limit(limit)
        result = await self.session.execute(paginated_query)

        return {
            "items": result.scalars().all(),
            "next_page": page + 1 if total_pages and page < total_pages else None,
            "prev_page": page - 1 if page > 1 else None,
            "total_pages": total_pages,
            "total_count": total_count,
        }

    async def _add(self, instance: T) -> T:
        try:
            self.session.add(instance)
            await self.session.commit()
            await self.session.refresh(instance)
        except Exception:
            await self.session.rollback()
            raise

        return instance

    def _order_by(self, query: Select[T], order_by: Optional[str | list[str]] = None) -> Select[T]:
        if not order_by:
            return query

        if isinstance(order_by, str):
            order_by = [order_by]

        for field in order_by:
            descending = field.startswith("-")
            field_name = field[1:] if descending else field

            if hasattr(self.model, field_name):
                column = getattr(self.model, field_name)
                query = query.order_by(desc(column) if descending else asc(column))

        return query
