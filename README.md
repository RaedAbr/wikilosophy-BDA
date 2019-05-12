# wikilosophy-BDA

***Ludovic Gindre & Raed Abdennadher & Edward Ransome***

***

## 1. Dataset

Dans ce projet, nous avons utilisé la base de données des articles *Wikipédia* jusqu'au **1er janvier 2019**. Cette base de données est sous forme d'un fichier `xml` compressé en format `bzip2`. La taille de ce fichier est **16,7Go**. La structure `xml` de ce fichier est la suivante:

```xml
<page>
    <title>...</title>
    <ns>...</ns>
    <id>...</id>
    <revision>
        <id>...</id>
        <parentid>...</parentid>
        <timestamp>...</timestamp>
        <contributor>
            <username>...</username>
            <id>...</id>			
        </contributor>
        <comment>...</comment>
        <model>...</model>
        <format>...</format>
        <text>...</text>
        <sha1>...</sha1>		
    </revision>	
</page>
```

Avec ce fichier, on a son index. C'est un fichier texte contenant `n` lignes, avec `n` le nombre total des pages de Wikipédia. Chaque ligne est sous la forme suivante:

```
offset:pageId:pageTitle
```

* `offset`: représente l'offset (en octets) dans le fichier de base, à partir duquel on trouve la page ayant l'identifiant `pageId`
* `pageId`: représente l'identifient de la page
* `pageTitle`: représente le titre de la page

![](./Screenshots/photo5935959929573716216.jpg)

Pour récupérer le contenu d'une page, il faut créer une petite partition en utilisant la commande shell `dd`, décompresser cette partition, et chercher séquentiellement la page en utilisant son `id`.

Exemple: pour récupérer le contenu de la page ***ASCII***, il faut créer une partition à partir de l'offset **615** jusqu'au l'offset **644151** du fichier de base, décompresser cette partition, puis la parcourir séquentiellement pour arriver à la page ***ASCII***

## 3. Questions d'analyse

- Calculer le chemin le plus court entre deux pages
- Quels sont les plus grand Hub (page avec le plus grand nombre de liens sortants) et les plus grandes Autorité (page avec le plus grand nombre de liens entrants) par catégorie.
- Quels sont les mots les plus utilisés sur Wikipédia (Stop-words exclus)
- Fournir le mot suivant le plus probable d'un mot donné