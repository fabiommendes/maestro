grades = {{ grades }};
levels = { "SR": "0.0", "II": "1.0", "MI": "2.0", "MM": "3.0", "MS": "4.0", "SS": "5.0" }

$ = jQuery;
$("#notas-turma tr").each(function () {
    ref = $(this).find("td:nth-child(2)").text().trim();
    grade = grades[ref];
    val = levels[grade];
    if (grade != undefined) {
        $(this).find("select").val(grade);
    }
});

console.log(": )")