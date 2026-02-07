# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools.test_execute

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import pytest
from netrun.tools._models import ActionConfig, ActionContext
from netrun.tools._execute import execute_action, execute_command

# %%
#|export
@pytest.mark.asyncio
async def test_execute_command_simple():
    ctx = ActionContext()
    result = await execute_command("echo hello", ctx)
    assert result.success
    assert result.exit_code == 0
    assert "hello" in result.stdout


@pytest.mark.asyncio
async def test_execute_command_with_env():
    ctx = ActionContext(node_name="TestNode")
    result = await execute_command("echo $NODE_NAME", ctx)
    assert result.success
    assert "TestNode" in result.stdout


@pytest.mark.asyncio
async def test_execute_command_failure():
    ctx = ActionContext()
    result = await execute_command("exit 1", ctx)
    assert not result.success
    assert result.exit_code == 1


@pytest.mark.asyncio
async def test_execute_command_bad_working_dir():
    ctx = ActionContext(working_directory="/nonexistent/dir")
    with pytest.raises(ValueError, match="Working directory does not exist"):
        await execute_command("echo hello", ctx)


@pytest.mark.asyncio
async def test_execute_action_delegates():
    action = ActionConfig(id="test", label="Test", command="echo action_works")
    ctx = ActionContext()
    result = await execute_action(action, ctx)
    assert result.success
    assert "action_works" in result.stdout


@pytest.mark.asyncio
async def test_execute_command_resolved_command():
    ctx = ActionContext(node_name="MyNode")
    result = await execute_command("echo $NODE_NAME", ctx)
    assert result.resolved_command == "echo MyNode"


@pytest.mark.asyncio
async def test_execute_command_timeout():
    ctx = ActionContext()
    result = await execute_command("sleep 10", ctx, timeout=0.1)
    assert not result.success
    assert result.timed_out
    assert result.exit_code == -1
