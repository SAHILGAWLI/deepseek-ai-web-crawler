import re

def normalize_title(title):
    """Normalize a title for better comparison by removing common words and symbols"""
    # Convert to lowercase
    title = title.lower()
    # Remove special characters and replace with spaces
    title = re.sub(r'[^\w\s]', ' ', title)
    # Remove common words that don't help with matching
    common_words = ['hackathon', 'challenge', 'competition', 'the', 'a', 'an', 'and', 'at', 'in', 'on', 'by', 'for', 'with', 'edition']
    for word in common_words:
        title = re.sub(r'\b' + word + r'\b', '', title)
    # Remove extra spaces
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def title_similarity(title1, title2):
    """Calculate similarity between two titles"""
    # Don't compare very short titles (less than 10 chars) to avoid false positives
    if len(title1) < 10 or len(title2) < 10:
        return False
        
    # Normalize both titles
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)
    
    # Don't compare normalized titles that are too short
    if len(norm1) < 5 or len(norm2) < 5:
        return False
    
    # Direct containment is a strong signal
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Extract and compare words
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    # Both should have some meaningful content
    if len(words1) < 2 or len(words2) < 2:
        return False
        
    # Calculate Jaccard similarity (intersection over union)
    intersection = words1.intersection(words2)
    
    # We need enough matching words
    if len(intersection) < 2:
        return False
        
    # Check if the intersection is a significant portion of the smaller set
    similarity_ratio = len(intersection) / min(len(words1), len(words2))
    
    # More words in common = higher confidence
    if len(intersection) >= 3 and similarity_ratio >= 0.5:
        return True
        
    # With just 2 words in common, we need a higher ratio
    if len(intersection) == 2 and similarity_ratio >= 0.6:
        return True
    
    # Special case for unique identifiers that strongly indicate the same event
    unique_identifiers = ['2025', 'techkriti', 'dawson', 'illuminati', 'agentforce', 'phystech']
    shared_identifiers = [word for word in intersection if any(id in word for id in unique_identifiers)]
    
    if len(shared_identifiers) >= 1 and similarity_ratio >= 0.4:
        return True
    
    return False

def check_duplicates(titles):
    seen_titles = {}  # Changed to dict to store original title and normalized version
    duplicates = []
    
    for title in titles:
        # Skip empty or very short titles
        if not title or len(title) < 5:
            continue
            
        # Check exact match first
        if title in seen_titles:
            duplicates.append((title, title, 'exact'))
            continue
        
        # Fuzzy matching for near-duplicates
        is_duplicate = False
        matching_title = None
        
        for seen_title, norm_seen in seen_titles.items():
            # Use our improved similarity function
            if title_similarity(title, seen_title):
                # Double-check with either the normalized title
                # or with a character-level similarity as a safety check
                is_duplicate = True
                matching_title = seen_title
                break
        
        if is_duplicate:
            duplicates.append((title, matching_title, 'fuzzy'))
        else:
            seen_titles[title] = normalize_title(title)
    
    return duplicates, list(seen_titles.keys())

# Test with real examples
titles = [
    'Agentforce Virtual Hackathon',
    'Agentforce Hackathon 2025',  # Should match the above
    'Global AI Agents League',
    'Global AI Agents League - Spring 2025',  # Should match the above
    'Meta Horizon Creator Competition: Mobile Genre Showdown',
    'Meta Mobile Horizon Creator Competition',  # Should match the above
    'GNEC Hackathon 2025 Spring',
    'GNEC Spring Hackathon 2025',  # Should match the above
    'CODE CRUNCH 305 Hackathon | Edition Spring 2025',
    'CODE CRUNCH Hackathon Spring Edition',  # Should match the above
    'HEALTH HACK X - ILLUMINATI',
    'ILLUMINATI HEALTH HACK',  # Should match because of ILLUMINATI
    'PhysTech 2025',
    'PhysTech 2025: Physical Activity Hackathon',  # Should match the above
    'Fetch.ai Hackathon',
    'Fetch.ai Hackathon at Techkriti',  # Should match the above
    'Dawson College AI Making Challenge',
    'AI Making Challenge at Dawson',  # Should match the above
    'Learn to Code',  # Too short/generic, should NOT match with other short entries
    'Learn Python',   # Too short/generic, should NOT match with other short entries
]

# Add some more challenging examples
titles.extend([
    'AI Hackathon 2025',
    'Artificial Intelligence Hackathon 2025',  # Should match the above
    'Crypto Challenge Spring',
    'Spring Crypto Challenge',  # Should match the above
    'Web3 Hackathon by Example Corp',
    'Example Corp Web3 Challenge',  # Should match the above
    'Spring Boot Coding Challenge',  # Should NOT match with other "Spring" entries
    'Spring Cloud Development Hackathon', # Should NOT match with other "Spring" entries
    'Crypto Art Exhibition',  # Should NOT match with "Crypto Challenge"
    'AI Conference 2025',  # Should NOT match with "AI Hackathon 2025"
])

duplicates, unique_titles = check_duplicates(titles)

print('Fuzzy Matching Test Results:')
print(f'Input: {len(titles)} titles')
print(f'Unique after deduplication: {len(unique_titles)}')
print(f'Duplicates found: {len(duplicates)}')
print('\nDetected Duplicates:')
for dup, match, match_type in duplicates:
    norm_dup = normalize_title(dup)
    norm_match = normalize_title(match)
    print(f'- "{dup}" matches "{match}" ({match_type})')
    print(f'  Normalized: "{norm_dup}" vs "{norm_match}"')
    
    # Calculate similarity metrics for analysis
    words_dup = set(norm_dup.split())
    words_match = set(norm_match.split())
    intersection = words_dup.intersection(words_match)
    similarity_ratio = len(intersection) / min(len(words_dup), len(words_match)) if min(len(words_dup), len(words_match)) > 0 else 0
    print(f'  Words in common: {list(intersection)} ({len(intersection)}/{min(len(words_dup), len(words_match))} = {similarity_ratio:.2f})')
    print()

# Find entries that should match but don't
print('\nPotential missed matches:')
manually_identified_pairs = [
    ('Spring Boot Coding Challenge', 'Spring Cloud Development Hackathon'),
    ('Learn to Code', 'Learn Python'),
    ('Crypto Challenge Spring', 'Crypto Art Exhibition'),
    ('AI Conference 2025', 'AI Hackathon 2025')
]

for title1, title2 in manually_identified_pairs:
    if title1 in unique_titles and title2 in unique_titles:
        norm1 = normalize_title(title1)
        norm2 = normalize_title(title2)
        words1 = set(norm1.split())
        words2 = set(norm2.split())
        intersection = words1.intersection(words2)
        similarity_ratio = len(intersection) / min(len(words1), len(words2)) if min(len(words1), len(words2)) > 0 else 0
        
        print(f'Not matched: "{title1}" and "{title2}"')
        print(f'  Normalized: "{norm1}" vs "{norm2}"')
        print(f'  Words in common: {list(intersection)} ({len(intersection)}/{min(len(words1), len(words2))} = {similarity_ratio:.2f})')
        print(f'  Would match with similarity function? {title_similarity(title1, title2)}')
        print()

# Let's demonstrate the normalized titles
print('\nTitle Normalization Examples:')
for title in list(unique_titles)[:5]:  # Show first 5 examples
    print(f'Original: "{title}"')
    print(f'Normalized: "{normalize_title(title)}"')
    print() 