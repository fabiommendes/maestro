from maestro.grades import markdown
import json


class TestExampleDocuments:
    def test_parse_external_competencies_assignments(self, data):
        src = data("exercise-list.md")
        expect = json.loads(data("exercise-list.json"))
        result = markdown.parse_external_competencies_assignments(src)
        assert result == expect
