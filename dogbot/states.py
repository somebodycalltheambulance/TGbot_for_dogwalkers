# dogbot/states.py
from aiogram.fsm.state import State, StatesGroup

class OrderStates(StatesGroup):
    choosing_service = State()
    choosing_walk_type = State()
    pet_name = State()
    pet_size = State()
    area = State()       
    when_at = State()
    duration_min = State()
    address = State()
    budget = State()
    comment = State()
    confirming = State()

class WorkStates(StatesGroup):
    collecting_name = State()
    collecting_phone = State()
    collecting_exp = State()
    collecting_areas = State() 

class ProposalStates(StatesGroup):
    waiting_price = State()
    waiting_note = State()


