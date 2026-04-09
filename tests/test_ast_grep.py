"""
Tests for the ast-grep tools implementation.
"""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

# Import the tools module - adjust the import path as needed
from src.tools.ast_grep.tools import register_tools, validate_file_path, run_ast_grep_command

# Create a mock MCP instance for testing
class MockMCP:
    def __init__(self):
        self.registered_tools = {}
    
    def tool(self):
        def decorator(func):
            self.registered_tools[func.__name__] = func
            return func
        return decorator

# Create a mock Context for testing
class MockContext:
    async def info(self, message):
        pass
    
    async def error(self, message):
        pass
    
    async def warning(self, message):
        pass
    
    async def report_progress(self, current, total):
        pass

@pytest.fixture
def mcp():
    return MockMCP()

@pytest.fixture
def context():
    return MockContext()

@pytest.fixture
def temp_project_dir():
    """Create a temporary directory for testing project path operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test file
        test_file_path = os.path.join(temp_dir, "test_file.cs")
        with open(test_file_path, "w") as f:
            f.write("public class TestClass { }")
        
        yield temp_dir

def test_validate_file_path(temp_project_dir):
    """Test the validate_file_path function."""
    # Test with valid file
    valid_path = "test_file.cs"
    full_path = validate_file_path(valid_path, temp_project_dir)
    assert os.path.isfile(full_path)
    
    # Test with non-existent file
    with pytest.raises(ValueError, match="does not exist"):
        validate_file_path("non_existent.cs", temp_project_dir)
    
    # Test with path outside project directory
    with pytest.raises(ValueError, match="outside the project directory"):
        validate_file_path("../test.cs", temp_project_dir)

@patch("subprocess.run")
def test_run_ast_grep_command(mock_run):
    """Test the run_ast_grep_command function."""
    # Mock subprocess.run return value
    mock_process = MagicMock()
    mock_process.stdout = '{"result": "success"}'
    mock_process.returncode = 0
    mock_run.return_value = mock_process
    
    # Test with JSON output
    result = run_ast_grep_command(["ast-grep", "run"], "/tmp", expect_json=True)
    assert result == {"result": "success"}
    
    # Test without JSON output
    mock_process.stdout = "Command executed successfully"
    result = run_ast_grep_command(["ast-grep", "run"], "/tmp", expect_json=False)
    assert result["stdout"] == "Command executed successfully"
    assert result["returncode"] == 0

@pytest.mark.asyncio
async def test_set_project_path(mcp, context):
    """Test the ast_grep_set_project_path tool."""
    # Register the tools
    register_tools(mcp)
    
    # Get the set_project_path function
    set_project_path = mcp.registered_tools["ast_grep_set_project_path"]
    
    # Test with valid path (using monkeypatch to avoid actual directory checks)
    with patch("os.path.isdir", return_value=True):
        result = await set_project_path("/valid/path", context)
        assert result["success"] is True
        assert result["project_path"] == "/valid/path"
    
    # Test with invalid path
    with patch("os.path.isdir", return_value=False):
        result = await set_project_path("/invalid/path", context)
        assert result["success"] is False
        assert "Directory not found" in result["error"]

@pytest.mark.asyncio
async def test_parse_code(mcp, context):
    """Test the ast_grep_parse_code tool."""
    # Register the tools
    register_tools(mcp)
    
    # Get the parse_code function
    parse_code = mcp.registered_tools["ast_grep_parse_code"]
    
    # Mock run_ast_grep_command to avoid actual command execution
    with patch("src.tools.ast_grep.tools.run_ast_grep_command") as mock_run:
        mock_run.return_value = {"matches": []}
        
        # Test with valid code
        result = await parse_code("public class Test {}", "csharp", context)
        assert result["success"] is True
        
        # Verify temp file creation and cleanup
        mock_run.assert_called_once()
        cmd_args = mock_run.call_args[0][0]
        assert cmd_args[0] == "ast-grep"
        assert cmd_args[1] == "run"

@pytest.mark.asyncio
async def test_find_pattern(mcp, context, temp_project_dir):
    """Test the ast_grep_find_pattern tool."""
    # Register the tools
    register_tools(mcp)
    
    # Get the find_pattern function
    find_pattern = mcp.registered_tools["ast_grep_find_pattern"]
    
    # Set the global current_project_path
    import src.tools.ast_grep.tools
    src.tools.ast_grep.tools.current_project_path = temp_project_dir
    
    # Mock run_ast_grep_command to avoid actual command execution
    with patch("src.tools.ast_grep.tools.run_ast_grep_command") as mock_run:
        mock_run.return_value = [{"text": "public class TestClass", "start": 0, "end": 23}]
        
        # Test with valid pattern
        result = await find_pattern("test_file.cs", "class $NAME", None, context)
        assert result["success"] is True
        assert result["count"] == 1
        assert len(result["matches"]) == 1

@pytest.mark.asyncio
async def test_replace_pattern(mcp, context, temp_project_dir):
    """Test the ast_grep_replace_pattern tool."""
    # Register the tools
    register_tools(mcp)
    
    # Get the replace_pattern function
    replace_pattern = mcp.registered_tools["ast_grep_replace_pattern"]
    
    # Set the global current_project_path
    import src.tools.ast_grep.tools
    src.tools.ast_grep.tools.current_project_path = temp_project_dir
    
    # Mock functions to avoid actual file operations
    with patch("src.tools.ast_grep.tools.run_ast_grep_command") as mock_run, \
         patch("builtins.open", create=True), \
         patch("shutil.copy2"):
        
        # Mock file content check
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = "public class TestClass : IInterface { }"
        with patch("builtins.open", return_value=mock_file):
            
            # Test with valid replacement
            result = await replace_pattern(
                "test_file.cs", 
                "class TestClass", 
                "class TestClass : IInterface", 
                None, 
                context
            )
            assert result["success"] is True
            assert "backup_path" in result

# Add more tests for other tools (run_yaml_rule, scan_project, initialize_project, test_rule)
