#
# makefile for generating paper.ps
#

LATEX = pdflatex
#LATEX = latex2e
#LATEX = latex

FILES   = introduction.tex background.tex design.tex evaluation.tex results.tex related.tex conclusions.tex ack.tex paper.tex paper.bib

BIB     = paper.bib

FIGS 	=  $(wildcard figures/*.pdf figures/*.png graphs/*.pdf graphs/*.png images/*.pdf images/*.png)

default: paper.pdf

paper.pdf: $(FILES) $(FIGS) $(BIB)
	-$(LATEX) -shell-escape paper
	-bibtex paper
	-$(LATEX) -shell-escape paper
	-$(LATEX) -shell-escape paper
	-$(LATEX) -shell-escape paper


clean:
	rm -f *.log *.aux *.blg *.ps *.pdf *.dvi *.bbl images/*converted-to* images/epsImages/*converted-to *figures/*converted-to* *.tex~ *.bib~ *.out *.eps

wc:
	@cat paper.tex | detex | wc -w

up:     update

update:
	git status
	git pull

cc: clean commit

commit:
	git status
	git commit -m "text modification" -a
	git push

status:
	git status
