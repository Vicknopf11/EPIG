import cv2
import numpy as np
from pathlib import Path
import networkx as nx
from tqdm import tqdm

IMG_DIR = Path("previews_200")

def load_images():
    paths = sorted(IMG_DIR.glob("EFTA*.png"))
    images = []
    for p in paths:
        img = cv2.imread(str(p), cv2.IMREAD_GRAYSCALE)
        images.append((p.name, img))
    return images

def compute_orb_features(images):
    orb = cv2.ORB_create(nfeatures=2000)
    feats = {}
    for name, img in images:
        kp, des = orb.detectAndCompute(img, None)
        feats[name] = des
    return feats

def match_score(des1, des2):
    if des1 is None or des2 is None:
        return 0
    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
    matches = bf.match(des1, des2)
    return len(matches)

def build_graph(features, threshold=150):
    names = list(features.keys())
    G = nx.Graph()

    for n in names:
        G.add_node(n)

    for i in tqdm(range(len(names))):
        for j in range(i+1, len(names)):
            score = match_score(features[names[i]], features[names[j]])
            if score > threshold:
                G.add_edge(names[i], names[j], weight=score)

    return G

def main():
    images = load_images()
    feats = compute_orb_features(images)
    G = build_graph(feats, threshold=150)

    print("Nodes:", G.number_of_nodes())
    print("Edges:", G.number_of_edges())

    components = list(nx.connected_components(G))
    print("Rooms detected:", len(components))

    for i, comp in enumerate(components):
        print(f"Room {i}: {len(comp)} images")

if __name__ == "__main__":
    main()
