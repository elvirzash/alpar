#include <stdio.h>
#include <stdlib.h>

int main(void)	{
        int a[10][10], b[10][10], c[10][10];
        int i, j, m, n, x, y, k;

        printf ("\nMasukan Dimensi Matrix A (nxm): ");
        scanf ("%dx%d", &m, &n);

        for (i = 0; i < m; i++)
        {
                for (j = 0; j < n; j++)
                {
                        printf (" a[%d][%d] = ", (i+1), (j+1));
                        scanf ("%d", &a[i][j]);
                }
                printf ("\n");
        }

        printf ("\nMasukan Dimensi Matrix B (nxm): ");
        scanf ("%dx%d", &x, &y);

        for (i = 0; i < x; i++)
        {
                for (j = 0; j < y; j++)
                {
                        printf (" b[%d][%d] = ", (i+1), (j+1));
                        scanf ("%d", &b[i][j]);
                }
                printf ("\n");
        }

        if ( n != x)
        {
                printf ("\n Error!! Rows of a don't match with columns of b.");
                printf ("\n Multiplication Impossible.");
                exit(EXIT_FAILURE);
        }
        else
        {
                for (i = 0; i < m; i++)
                {
                        for (j = 0; j < y; j++)
                        {
                                c[i][j] = 0;
                                for (k = 0; k < n; k++)
                                {
                                        c[i][j]+=a[i][k]*b[k][j];
                                }
                        }
                }
        }

        printf ("\n The resultant c is:\n");
        for (i = 0; i < m; i++)
        {
                for (j = 0; j < y; j++)
                {
                        printf (" c[%d][%d] = %d", (i+1), (j+1), c[i][j]);
                }
                printf ("\n");
        }

        return 0;
}
