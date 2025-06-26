# ChatController.py

import asyncio
import nltk, math, io, urllib.request, csv, random
from collections import Counter
from nltk.corpus import wordnet
import pymorphy2

from reportingController import get_report

# Если корпуса ещё не скачаны
nltk.download('punkt', quiet=True)
nltk.download('wordnet', quiet=True)
nltk.download('omw-1.4', quiet=True)

# Морфологический разбор
morph = pymorphy2.MorphAnalyzer()

def lemmatize(tokens):
    return [morph.parse(t)[0].normal_form for t in tokens]

def expand_with_synonyms(lemmas):
    s = set(lemmas)
    for w in lemmas:
        for syn in wordnet.synsets(w):
            for lemma in syn.lemmas():
                norm = morph.parse(lemma.name().lower().replace('_', ' '))[0].normal_form
                s.add(norm)
    return s

def text_to_vector(text):
    tokens  = nltk.word_tokenize(text.lower())
    lemmas  = lemmatize(tokens)
    expanded = expand_with_synonyms(lemmas)
    return Counter(expanded), set(lemmas)

def cosine_similarity(v1, v2):
    inter = set(v1) & set(v2)
    num   = sum(v1[x] * v2[x] for x in inter)
    den   = math.sqrt(sum(v**2 for v in v1.values())) \
          * math.sqrt(sum(v**2 for v in v2.values()))
    return num / den if den else 0.0

def load_qa_from_sheet():
    SHEET_ID = "1NRGPRwpMyXTe9LhS4adwfPo7nyx68GqweYdAdqo3LpM"
    GID      = "384502621"
    url      = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    resp     = urllib.request.urlopen(url)
    reader   = csv.DictReader(io.TextIOWrapper(resp, encoding='utf-8'))
    qa       = []
    for row in reader:
        questions = [q.strip() for q in row['Варианты вопросов'].split(';') if q.strip()]
        answers   = [a.strip() for a in row['Варианты ответов'].split(';') if a.strip()]
        keywords  = [lemmatize([kw.strip()])[0] for kw in row.get('Ключевые слова', '').split(';') if kw.strip()]
        for q in questions:
            qa.append({
                'question': q,
                'vector': text_to_vector(q)[0],
                'answers': answers,
                'keywords': set(keywords)
            })
    return qa


def build_question_vectors():
    return load_qa_from_sheet()


# Глобальный стейт
question_vectors = build_question_vectors()
chat_listener_active = False
THRESHOLD = 0.6

def find_answer(user_text, user_id=None, send_func=None):
    v_user, lemmas = text_to_vector(user_text)

    candidates = []
    for item in question_vectors:
        match_count = len(item['keywords'] & lemmas)
        if match_count > 0:
            candidates.append((match_count, cosine_similarity(v_user, item['vector']), item))

    if candidates:
        best = sorted(candidates, key=lambda x: (-x[0], -x[1]))[0][2]
        answer = random.choice(best['answers'])
    else:
        best = max(question_vectors, key=lambda t: cosine_similarity(v_user, t['vector']))
        score = cosine_similarity(v_user, best['vector'])
        answer = random.choice(best['answers']) if score >= THRESHOLD else None

    print(answer)
    # --- Генерация отчета
    if answer and answer.startswith("Генерируемый ответ системой."):
        parts = answer.replace("Генерируемый ответ системой.", "").strip().split()
        if len(parts) == 2:
            mode, city = parts[0], parts[1]
        else:
            mode, city = parts[0] + " " + parts[1], parts[2]
        report = get_report(city, mode)
        print(report)

        if user_id and send_func:
            asyncio.create_task(send_func(user_id, f"<b>Отчет {mode} в {city}:</b>\n\n{report}"))
            return None, True  # Отчет отправлен
        else:
            return f"<b>Отчет {mode} в {city}:</b>\n\n{report}", False

    return answer, False


if __name__ == '__main__':
    test_questions = [
        "Расскажи про акции",
        "Есть ли акция на товар?",
        "Есть ли скидка на товар?"
    ]

    for i, question in enumerate(test_questions, start=1):
        answer = find_answer(question)
        print(f"\n[{i}] Вопрос: {question}")
        print(f"Ответ:  {answer if answer else 'Нет подходящего ответа'}")
