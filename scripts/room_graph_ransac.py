import cv2
import numpy as np
from pathlib import Path
import networkx as nx
from tqdm import tqdm

IMG_DIR = Path("previews_200")

def load_images():
    paths = sorted(IMG_DIR.glob("EFTA*.png"))
    items = []
    for p in paths:
        img = cv2.imread(str(p), cv2.IMREAD_GRAYSCALE)
        items.append((p.name, img))
    return items

def compute_orb(images):
    orb = cv2.ORB_create(nfeatures=2500)
    feats = {}
    kps = {}
    for name, img in images:
        kp, des = orb.detectAndCompute(img, None)
        kps[name] = kp
        feats[name] = des
    return kps, feats

def ransac_inliers(kp1, des1, kp2, des2):
    if des1 is None or des2 is None:
        return 0

    bf = cv2.BFMatcher(cv2.NORM_HAMMING)
    # knn match + ratio test
    knn = bf.knnMatch(des1, des2, k=2)
    good = []
    for m, n in knn:
        if m.distance < 0.75 * n.distance:
            good.append(m)

    if len(good) < 12:
        return 0

    pts1 = np.float32([kp1[m.queryIdx].pt for m in good])
    pts2 = np.float32([kp2[m.trainIdx].pt for m in good])

    # verifica geometrica (omografia)
    H, mask = cv2.findHomography(pts1, pts2, cv2.RANSAC, 5.0)
    if mask is None:
        return 0
    inliers = int(mask.sum())
    return inliers

def build_graph(names, kps, feats, inlier_threshold=18, limit_pairs=None):
    G = nx.Graph()
    for n in names:
        G.add_node(n)

    pairs = 0
    for i in tqdm(range(len(names))):
        for j in range(i + 1, len(names)):
            pairs += 1
            if limit_pairs and pairs > limit_pairs:
                break

            a, b = names[i], names[j]
            inl = ransac_inliers(kps[a], feats[a], kps[b], feats[b])
            if inl >= inlier_threshold:
                G.add_edge(a, b, weight=inl)

        if limit_pairs and pairs > limit_pairs:
            break

    return G

def main():
    images = load_images()
    print("Found:", len(images), "images in", IMG_DIR.resolve())
    names = [n for n, _ in images]

    kps, feats = compute_orb(images)

    # soglia iniziale: 25 inliers (da tarare)
    G = build_graph(names, kps, feats, inlier_threshold=25)

    print("Nodes:", G.number_of_nodes())
    print("Edges:", G.number_of_edges())

    comps = list(nx.connected_components(G))
    comps.sort(key=len, reverse=True)
    print("Rooms detected:", len(comps))
    for i, c in enumerate(comps[:10]):
        print(f"Room {i}: {len(c)} images")

if __name__ == "__main__":
    main()
