
import pytest
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram import Dispatcher, Bot
from dogbot.states import OrderStates, WorkStates

@pytest.fixture
def dp_bot():
    bot = Bot("12345:TEST")
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    return dp, bot

@pytest.mark.asyncio
async def test_work_states(dp_bot):
    dp, bot = dp_bot
    ctx = FSMContext(storage=dp.storage, key=("user", 1, "chat", 1))
    assert await ctx.get_state() is None
    await ctx.set_state(WorkStates.collecting_name)
    assert await ctx.get_state() == WorkStates.collecting_name
    await ctx.set_state(WorkStates.collecting_phone)
    assert await ctx.get_state() == WorkStates.collecting_phone
    await ctx.set_state(WorkStates.collecting_exp)
    assert await ctx.get_state() == WorkStates.collecting_exp
    await ctx.clear()
    assert await ctx.get_state() is None

@pytest.mark.asyncio
async def test_order_states(dp_bot):
    dp, bot = dp_bot
    ctx = FSMContext(storage=dp.storage, key=("user", 2, "chat", 2))
    assert await ctx.get_state() is None
    await ctx.set_state(OrderStates.choosing_service)
    assert await ctx.get_state() == OrderStates.choosing_service
    await ctx.set_state(OrderStates.choosing_walk_type)
    assert await ctx.get_state() == OrderStates.choosing_walk_type
    await ctx.set_state(OrderStates.collecting_description)
    assert await ctx.get_state() == OrderStates.collecting_description
