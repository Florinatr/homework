#include <iostream>
#include <vector>
#include <memory>
#include "./p0_starter.h"

int main()
{

    scudb::RowMatrix<int> mat1(3, 4);
    for (int i = 0;i < mat1.GetRowCount(); ++i)
    {
        for (int j= 0; j < mat1.GetColumnCount(); ++j)
        {
            mat1.SetElement(i, j, i + j);
        }
    }
    std::cout << "Matrix 1: " << std::endl;
    scudb::RowMatrixOperations<int>::Print(mat1);

    std::vector<int> vec34(3 * 4, 1);
    std::vector<int> vec33(3 * 3, 1);

    std::cout << "Filled Matrix 2" << std::endl;
    scudb::RowMatrix<int> mat2(3, 4);
    mat2.FillFrom(vec34);
    scudb::RowMatrixOperations<int>::Print(mat2);

    std::cout << "Filled Matrix 3" << std::endl;
    scudb::RowMatrix<int> mat3(4, 3);
    mat3.FillFrom(vec34);
    scudb::RowMatrixOperations<int>::Print(mat3);

    std::cout << "Filled Matrix 4" << std::endl;
    scudb::RowMatrix<int> mat4(3, 3);
    mat4.FillFrom(vec33);
    scudb::RowMatrixOperations<int>::Print(mat4);

    std::cout << "  ²âÊÔAdd  Matrix 1 + 2" << std::endl;
    scudb::RowMatrixOperations<int>::Print(*scudb::RowMatrixOperations<int>::Add(&mat1, &mat2));

    std::cout << "²âÊÔ Multiply Matrix 2 * 3" << std::endl;
    scudb::RowMatrixOperations<int>::Print(*scudb::RowMatrixOperations<int>::Multiply(&mat2, &mat3));

    std::cout << "²âÊÔGEMM Matrix  2*3 + 4" << std::endl;
    scudb::RowMatrixOperations<int>::Print(*scudb::RowMatrixOperations<int>::GEMM(&mat2, &mat3, &mat4));

    return 0;
}