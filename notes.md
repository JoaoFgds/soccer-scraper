# Conceitos (Resumo)

* **Ordenação de Habilidade ($R$):** Conjunto $\{r_1, \dots, r_n\}$ onde $r_1$ é o mais forte e $r_n$ o mais fraco.
* **Abordagem Oracle:** Força das equipes inferida diretamente pela classificação final no torneio.
* **Tabela ($\mathbf{S}_i$):** Permutação de $R_{-i}$ representando a ordem cronológica dos oponentes enfrentados.
* **Métrica de Equilíbrio ($B(\mathbf{S}_i)$):** Coeficiente de correlação de Spearman entre a tabela real $\mathbf{S}_i$ e a sequência ideal de desbalanceamento $R_{-i}$.
* **Classificação de Grupos:**
    * **$G_0$ (Balanceado):** Dificuldade uniformemente distribuída entre as rodadas.
    * **$G^{\pm}$ (Desbalanceado):** Concentração de oponentes fortes no início ($G^-$) ou no fim ($G^+$) da competição.
* **Teste U de Mann-Whitney:** Teste não paramétrico para comparar a distribuição das posições finais entre $G_0$ e os grupos desbalanceados.
* **Significância Direcional ($p < 0,05$):**
    * **Melhor Performance:** $Mediana(G_{teste}) < Mediana(G_0)$ (colocação numericamente menor).
    * **Pior Performance:** $Mediana(G_{teste}) > Mediana(G_0)$ (colocação numericamente maior).


## Lógica de Análise Estatística

* **Estratificação Espaço-Temporal:** Os testes são aplicados de forma isolada para cada combinação de Liga e Temporada (`league_name`, `season_year`) para garantir a homogeneidade das amostras.
* **Comparações Pareadas (Mann-Whitney U):** Execução de testes não paramétricos bicaudais (`two-sided`) em três frentes:
    * $G^-$ vs $G^0$: Impacto de iniciar com tabela fácil em relação ao equilíbrio.
    * $G^+$ vs $G^0$: Impacto de iniciar com tabela difícil em relação ao equilíbrio.
    * $G^-$ vs $G^+$: Contraste direto entre os extremos de desbalanceamento.
* **Taxa de Significância (Significance Rate):** Métrica agregada que calcula a porcentagem de temporadas onde o efeito do desbalanceamento foi estatisticamente relevante ($p < 0,05$).


        Obs.: O teste de Mann Whitney é executado por liga por temporada,
         isso está correto?


# Plots para entender o impacto do desbalanceamento no tournament efficacy

### Significância direcional
* A idéia era 

Directional Significance Diverging Bar Chart

This script creates a diverging bar chart showing the directional impact
of schedule imbalance on tournament performance. For seasons where the
Mann-Whitney U test shows statistical significance (p < 0.05), it determines
whether the unbalanced group (G- or G+) performed better or worse than
the balanced group (G0) by comparing median final positions.

Logic:
- If p < 0.05 AND Median(G_test) < Median(G0): Better Performance (lower rank = higher position)
- If p < 0.05 AND Median(G_test) > Median(G0): Worse Performance (higher rank = lower position)


### Delta de cliff

* cada combinação de torneio & ano gera um valor de delta de cliff na comparação de dois grupos (g- vs g0 por exemplo)

* Basicamente pega um grupo e vê quantas vezes um item desse grupo terminou num ranking maior/menor que os itens do outro grupo. Um delta = 1 significa que todos os itens do grupo desbalanceado terminaram em posições melhores que o grupo balanceado.



todo

1. reorganizar o codigo para parametrizar os resultados e definir o threshold como coef de spearman ou p-valor do coeficiente (configuravel)

2. ter uma tabela com o tamanho dos grupos

3. Fazer um df como um sumário dos testes que fazemos