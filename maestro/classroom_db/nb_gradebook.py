"""
Functions to manipulate nbgrader gradebook databases.
"""
import os

import pandas as pd


class NbGradebook:
    """
    Controls a gradebook.db database file.
    """

    def __init__(self, path='gradebook.db'):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        self.path = path

    # Tables
    assignment = property(lambda self: self.table('assignment'))
    comment = property(lambda self: self.table('comment'))
    grade = property(lambda self: self.table('grade'))
    grade_cell = property(lambda self: self.table('grade_cell'))
    notebook = property(lambda self: self.table('notebook'))
    solution_cell = property(lambda self: self.table('solution_cell'))
    source_cell = property(lambda self: self.table('source_cell'))
    student = property(lambda self: self.table('student'))
    submitted_assignment = property(lambda self: self.table('submitted_assignment'))
    submitted_notebook = property(lambda self: self.table('submitted_notebook'))

    def table(self, table) -> pd.DataFrame:
        """
        Return table as data frame.
        """
        df = pd.read_sql(table, f'sqlite:///{self.path}')
        if 'id' in df.columns:
            df.index = df.pop('id')
        return df

    def query(self, query, **kwargs) -> pd.DataFrame:
        """
        Return table as data frame.if __name__ == '__main__':
        """
        return pd.read_sql_query(query, f'sqlite:///{self.path}', **kwargs)

    @property
    def max_grades(self):
        return self.query('''
            SELECT 
                sum(max_score) as score,
                nb.name as notebook

            FROM 
                grade_cell as gc
                LEFT JOIN 
                    notebook as nb ON nb.id = gc.notebook_id

            GROUP BY 
                gc.notebook_id

            ORDER BY
                score DESC
            ''', index_col='notebook').sort_index()

    def gradebook(self, normalized=False, full=True):
        df = self.query('''
            SELECT
                st.id as school_id,
                sum(coalesce(g.auto_score, 0) + coalesce(g.manual_score, 0) + coalesce(g.extra_credit, 0)) as score,
                nb.name as notebook

            FROM 
                grade as g
                LEFT JOIN 
                    submitted_notebook AS sn ON sn.id = g.notebook_id
                LEFT JOIN 
                    submitted_assignment AS sa ON sa.id = sn.assignment_id
                LEFT JOIN 
                    student AS st ON st.id = sa.student_id
                LEFT JOIN 
                    notebook AS nb ON nb.id = sn.notebook_id

            GROUP BY 
                g.notebook_id

            ORDER BY
                notebook ASC, score DESC
            ''')
        # Remove duplicates before pivot
        df = df.groupby(['school_id', 'notebook']).max()
        df[['school_id', 'notebook']] = df.index.to_frame()

        grades = df.pivot('school_id', 'notebook', 'score').fillna(0)

        # Normalize with maximum grade possible for activity
        if normalized:
            max_grades = self.max_grades
            maximum = dict(zip(max_grades.index, max_grades.values))
            for col in grades.columns:
                if col in maximum:
                    grades[col] /= maximum[col]
        if full:
            cols = ['first_name', 'last_name', 'email']
            grades[cols] = self.student[cols]
            grades = grades[[*cols, *grades.columns[:-3]]]
        return grades
