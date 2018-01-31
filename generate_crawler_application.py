from spacetime.common.crawler_generator import generate
ids, nos = zip(*(l.strip().split(",") for l in open("team.txt") if l and not l.strip().startswith("#")))
print "Student IDs provided:", ids
print "Student numbers provided:", nos

app_id = "".join(i.title() for i in ids)
print "Your app id is: ", app_id

useragentstring, filename, typenames = generate(app_id)
print "Your User Agent String is going to be: ", useragentstring
print "Your datamodel file name is: ", filename
print "Your generated classes and functions are: ", typenames

open("generated_details.txt", "w").write(
    "\n".join([useragentstring, app_id]))

print "Files have been written."
print "Details have also been stored in generated_details.txt."