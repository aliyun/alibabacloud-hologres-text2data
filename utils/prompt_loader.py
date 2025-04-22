# utils/prompt_loader.py
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

class PromptLoader:
    def __init__(self):
        self.env = Environment(
            loader=FileSystemLoader(Path(__file__).parent.parent / 'prompt_templates/sql_generation'),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def get_prompt(
        self, 
        db_type: str, 
        context: dict,
        limit: int = 100,
        user_custom_prompt: str | None = None  # New Custom Parameters
    ) -> str:
        try:
            template = self.env.get_template(f"{db_type.lower()}_prompt.jinja")
        except TemplateNotFound:
            template = self.env.get_template("base_prompt.jinja")
        
        # Inject custom prompts into the context
        context.update({
            'limit_clause': self._get_limit_clause(db_type),
            'optimization_rules': self._get_optimization_rules(db_type),
            'user_custom_prompt': user_custom_prompt,  
            'limit': limit
        })
        return template.render(context)
    
    def _get_limit_clause(self, db_type: str) -> str:
        clauses = {
            'mysql': "LIMIT n",
            'oracle': "ROWNUM <= n",
            'sqlserver': "TOP n",
            'hologres': "LIMIT n"
        }
        return clauses.get(db_type.lower(), "LIMIT 100")
    
    def _get_optimization_rules(self, db_type: str) -> str:
        rules = {
            'hologres': "- 分析使用EXPLAIN ANALYZE获得的执行计划"
        }
        return rules.get(db_type.lower(), "")

def test_prompt_loading():
    loader = PromptLoader()
    
    # TEST MySQL
    mysql_context = {
        'meta_data': 'mock_metadata',
        'query': 'mock_query',
        'db_type': 'hologres'  
    }
    mysql_prompt = loader.get_prompt('mysql', mysql_context)
    print("MySQL Prompt Output:\n", mysql_prompt)
    assert "LIMIT n" in mysql_prompt

if __name__ == '__main__':
    test_prompt_loading()