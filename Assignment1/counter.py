s = """
2 Candidate_Statements.f 0
2 Computer_Science.f 0
2 ECE_Course_Reviews.f 0
2 Economics.f 0
2 Graduate_Groups.f 1
2 Mathematics.f 3 
2 Private_Tutoring.f 0
2 Statistics.f 2
2 Teaching_Assistants.f 0
2 UCD_Honors_and_Prizes.f 1
2 What_I_Wish_I_Knew...Before_Coming_to_UC_Davis_Entomology.f 2
"""

s = s.strip().split("\n")
cnt = [0,0,0,0]
for i in s:
    i = i.strip().split(" ")
    i = int(i[-1])
    cnt[i] += 1

print(cnt, sum(cnt))
print(f'P: {cnt[1:]}/{sum(cnt)} = {sum(cnt[1:])}/{sum(cnt)} = {sum(cnt[1:])/sum(cnt)}, R {sum(cnt[1:])/100}')