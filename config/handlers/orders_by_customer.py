"""Beta-пример: полные итоги активных заказов по покупателям.

Источник orders: customer_id, amount, is_deleted_or_empty. После изменения источника
пересчитываем весь агрегат в одной транзакции. Это намеренно не инкрементальный
алгоритм: он корректно учитывает удаления и перенос заказа между покупателями.
Таблицы задаются явно, имена и типы нужно адаптировать к своей схеме NameMapper.
MartLoop сохраняет watermark после успешного коммита и сам арендует destination.
"""

from sqlalchemy import Column, MetaData, Numeric, String, Table, delete, func, insert, select

from onecdc.marts import MartContext, MartHandler


class OrdersByCustomer(MartHandler):
    destination = "orders_by_customer"
    sources = ["orders"]

    def setup(self, ctx: MartContext) -> None:
        destination_schema, destination_name = ctx.destination
        self.table = Table(destination_name, MetaData(),
                           Column("customer_id", String(255), primary_key=True),
                           Column("total", Numeric, nullable=False), schema=destination_schema)
        self.table.create(ctx.engine, checkfirst=True)
        source_schema, source_name = next(iter(ctx.sources))
        self.orders = Table(source_name, MetaData(), schema=source_schema,
                            autoload_with=ctx.engine)

    def process_changes(self, ctx: MartContext) -> None:
        totals = select(self.orders.c.customer_id, func.sum(self.orders.c.amount)).where(
            self.orders.c.is_deleted_or_empty.is_(False),
            self.orders.c.customer_id.is_not(None)).group_by(self.orders.c.customer_id)
        with ctx.engine.begin() as conn:
            conn.execute(delete(self.table))
            conn.execute(insert(self.table).from_select(["customer_id", "total"], totals))