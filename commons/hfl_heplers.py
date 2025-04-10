from datetime import datetime, timezone

from commons.utils import datetime_as_utc
from database.prisma import Json
from database.prisma.enums import HFLStatusEnum
from database.prisma.models import HFLState
from database.prisma.types import HFLStateUpdateInput
from dojo.protocol import HFLEvent, ScoreFeedbackEvent, TextFeedbackEvent


class HFLManager:
    @staticmethod
    async def create_state(
        original_task_id: str,
        current_task_id: str,
        status: HFLStatusEnum = HFLStatusEnum.TF_PENDING,
        selected_completion_id: str = None,
    ) -> HFLState:
        """Create initial HFL state."""
        # TODO: add data as needed
        initial_event = TextFeedbackEvent(
            task_id=current_task_id,
            iteration=1,
            timestamp=datetime_as_utc(datetime.now(timezone.utc)),
        )

        # Prepare data for creating the HFL state
        create_data = {
            "original_task_id": original_task_id,
            "current_task_id": current_task_id,
            "current_iteration": 1,
            "status": status,
            "events": [Json(initial_event.model_dump())],
        }

        # Add selected_completion_id if provided
        if selected_completion_id:
            create_data["selected_completion_id"] = selected_completion_id

        return await HFLState.prisma().create(data=create_data)

    @staticmethod
    async def update_state(
        hfl_state_id: str,
        updates: HFLStateUpdateInput,
        event_data: HFLEvent,
    ) -> HFLState:
        """Update HFL state and handle status transitions."""
        current_state = await HFLState.prisma().find_unique(where={"id": hfl_state_id})
        if not current_state:
            raise ValueError(f"No HFL state found with ID {hfl_state_id}")

        new_status = updates.get("status")
        if new_status:
            match new_status:
                case HFLStatusEnum.TF_COMPLETED:
                    return await HFLManager._handle_tf_completed(
                        current_state, updates, event_data
                    )
                case HFLStatusEnum.TF_PENDING:
                    return await HFLManager._handle_tf_pending(
                        current_state, updates, event_data
                    )
                case HFLStatusEnum.SF_PENDING:
                    return await HFLManager._handle_sf_pending(
                        current_state, updates, event_data
                    )
                case HFLStatusEnum.SF_COMPLETED:
                    return await HFLManager._handle_sf_completed(
                        current_state, updates, event_data
                    )
                case HFLStatusEnum.HFL_COMPLETED:
                    return await HFLManager._handle_hfl_completed(
                        current_state, updates, event_data
                    )

        return await HFLManager._update_state(current_state, updates, event_data)

    @staticmethod
    async def _handle_tf_pending(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Handle transition to TF_PENDING."""
        # TODO Add TF pending specific logic as needed
        return await HFLManager._update_state(state, updates, event_data)

    @staticmethod
    async def _handle_tf_completed(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Handle transition to TF_COMPLETED."""
        # TODO Add TF completion specific logic as needed
        return await HFLManager._update_state(state, updates, event_data)

    @staticmethod
    async def _handle_sf_pending(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Handle transition to SF_PENDING."""
        # TODO Add SF pending specific logic as needed

        updates["current_synthetic_req_id"] = (
            event_data.syn_req_id
        )  # Clear the current synthetic request ID
        return await HFLManager._update_state(state, updates, event_data)

    @staticmethod
    async def _handle_sf_completed(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Handle transition to SF_COMPLETED."""
        # TODO Add SF completion specific logic as needed
        return await HFLManager._update_state(state, updates, event_data)

    @staticmethod
    async def _handle_hfl_completed(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Handle transition to HFL_COMPLETED."""
        # TODO Add HFL completion specific logic as needed
        return await HFLManager._update_state(state, updates, event_data)

    @staticmethod
    async def _update_state(
        state: HFLState, updates: HFLStateUpdateInput, event_data: HFLEvent
    ) -> HFLState:
        """Core update logic used by all handlers."""
        if event_data:
            updates["events"] = {"push": [Json(event_data.model_dump())]}

        update_state = await HFLState.prisma().update(
            where={"id": state.id}, data=updates
        )

        if not update_state:
            raise ValueError(f"Failed to update HFL state with ID {state.id}")

        return update_state

    @staticmethod
    async def get_state(hfl_state_id: str) -> HFLState:
        """Get HFL state by ID."""
        state = await HFLState.prisma().find_unique(where={"id": hfl_state_id})
        if not state:
            raise ValueError(f"No HFL state found with ID {hfl_state_id}")
        return state

    @staticmethod
    async def get_update_state_operation(
        state_id: str, status_updates: HFLStateUpdateInput, event: ScoreFeedbackEvent
    ):
        return {
            "where": {"id": state_id},
            "data": HFLStateUpdateInput(
                status_updates,
                events={"push": [Json(event.model_dump())]},
                updated_at=datetime_as_utc(datetime.now(timezone.utc)),
            ),
        }
