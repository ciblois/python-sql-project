/* Possible new students: isAlumini? They have job? */
SELECT avg(overallScore), school
FROM project2.students
GROUP BY school;

SELECT sum(overall), school
FROM project2.students
GROUP BY school;

/* Avaliar a empregabilidade */
SELECT isAlumni, jobTitle, program
FROM project2.students
GROUP BY program
ORDER BY isAlumni DESC; 
/* O ideal nesse tipo de questionário seria ter um campo empregado ou não empregado obrigatório e caso a pessoa marque empregado, digitar o JobTittle */

SELECT isAlumni, jobTitle, program
FROM project2.students
WHERE jobTitle LIKE 'Unemployed';

SELECT jobTitle, jobSupport, school
FROM project2.students
WHERE jobTitle LIKE 'Data%'
GROUP BY jobSupport
ORDER BY jobSupport DESC;

SELECT jobTitle, jobSupport, school
FROM project2.students
WHERE jobTitle LIKE 'Web%'
GROUP BY jobSupport
ORDER BY jobSupport DESC;

SELECT jobTitle, jobSupport, school
FROM project2.students
WHERE jobTitle LIKE 'UX/UI%'
GROUP BY jobSupport
ORDER BY jobSupport DESC;
