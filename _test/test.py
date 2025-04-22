import re
def extract_sql_from_text(text):
    """
    Extract SQL content from ```sql ... ``` blocks in text, even if there is other content before or after
    """
    # Regex explanation:
    # \s*: Match possible spaces or line breaks at the beginning
    # (.*?): Non-greedy match of SQL content
    # Removed ^ and $ anchors to allow text before and after, only capturing valid code blocks
    pattern = r'```sql\s*(.*?)\s*```'
    matches = re.findall(pattern, text, flags=re.DOTALL)  # Use findall to capture all matches
    
    # Return first match or all matches based on requirements
    if matches:
        return [sql.strip() for sql in matches]  # Remove whitespace from beginning and end of SQL content
    return None

# Test cases
if __name__ == "__main__":
    test_cases = [
        "xxxxxx ```sql SELECT * FROM users``` xxxxxxx",
        "```sql\nINSERT INTO logs (message) VALUES ('Hello')\n```",
        "Other text ```sql UPDATE users SET name = 'John' WHERE id = 1``` Other text",
        "Multiple blocks ```sql DELETE FROM temp ``` and ```sql TRUNCATE temp_table ```"
    ]
    for i, text in enumerate(test_cases):
        print(f"Case {i+1}:")
        result = extract_sql_from_text(text)
        print(f"Input: {text}")
        print(f"Extraction result: {result}\n{'='*40}")